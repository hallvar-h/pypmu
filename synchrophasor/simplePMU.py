from synchrophasor.frame import ConfigFrame2
from synchrophasor.pmu_mod import PmuMod
import random
import time
import numpy as np
import socket
import queue


class SimplePMU:
    def __init__(self, ip, port, station_names, channel_names, pdc_id=1, channel_types=None, id_codes=None, publish_frequency=50, set_timestamp=True):

        self.station_names = station_names
        self.channel_names = channel_names
        self.channel_types = channel_types

        self.n_pmus = len(self.station_names)

        # Initialize PMUs
        self.ip = ip
        self.port = port
        self.pmu = PmuMod(ip=self.ip, port=self.port, set_timestamp=set_timestamp, data_rate=publish_frequency)
        # self.pmu.logger.setLevel("DEBUG")

        conf_kwargs = dict(
            pmu_id_code=pdc_id,  # PMU_ID
            time_base=1000000,  # TIME_BASE
            num_pmu=self.n_pmus,  # Number of PMUs included in recorded_pmu_data_raw frame
            data_format=(True, True, True, True),  # Data format - POLAR; PH - REAL; AN - REAL; FREQ - REAL;
            analog_num=0,  # Number of analog values
            digital_num=0,  # Number of digital status words
            an_units=[],  # (1, "pow")],  # Conversion factor for analog channels
            dig_units=[],  # (0x0000, 0xffff)],  # Mask words for digital status words
            f_nom=50,  # Nominal frequency
            cfg_count=1,  # Configuration change count
            data_rate=publish_frequency
        )

        other_channel_names = []

        if self.n_pmus == 1:
            self.n_phasors_per_pmu = len(self.channel_names[0])
            self.n_phasors = self.n_phasors_per_pmu
            conf_kwargs['id_code'] = id_codes[0] if id_codes is not None else 1
            conf_kwargs['station_name'] = station_names[0]  # 'PMU'
            conf_kwargs['ph_units'] = [(0, "v")]*self.n_phasors
            conf_kwargs['channel_names'] = self.channel_names[0] + other_channel_names
            conf_kwargs['phasor_num'] = self.n_phasors
        else:
            self.n_phasors_per_pmu = [len(channel_names_) for channel_names_ in self.channel_names]
            conf_kwargs['phasor_num'] = self.n_phasors_per_pmu
            if self.channel_types is not None:
                conf_kwargs['ph_units'] = [[(0, channel_type_) for channel_type_ in channel_type] for channel_type in self.channel_types]
            else:
                conf_kwargs['ph_units'] = [[(0, "v")]*n_phasors_per_pmu for n_phasors_per_pmu in self.n_phasors_per_pmu]

            if id_codes is not None:
                conf_kwargs['id_code'] = id_codes
            else:
                conf_kwargs['id_code'] = list(range(1, self.n_pmus + 1))

            conf_kwargs['station_name'] = self.station_names
            conf_kwargs['channel_names'] = [channel_names_ + other_channel_names for channel_names_ in self.channel_names]
            for key in ['data_format', 'analog_num', 'digital_num', 'an_units', 'dig_units', 'f_nom', 'cfg_count']:
                conf_kwargs[key] = [conf_kwargs[key]] * self.n_pmus

        cfg = ConfigFrame2(**conf_kwargs)

        self.pmu.set_configuration(cfg)
        self.pmu.set_header("My PMU-Stream")

        self.run = self.pmu.run

    def cleanup(self):
        # This does not always work...
        self.pmu.stop()
        # print('SimplePMU stopped.')
        # print('Connecting to listener socket')
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((self.ip, self.port))
        # print('Closing listener socket')
        self.pmu.socket.close()
        # print('Joining listener')
        self.pmu.listener.join()
        # print('Stopping PDC handler processes')
        for stop_event in self.pmu.client_stop_events:
            stop_event.set()

        # print('Emptying client buffers')
        # Empty queues (remaining items might cause main process to not exit)
        for i, client_buffer in enumerate(self.pmu.client_buffers):
            k = 0
            try:
                while True:
                    client_buffer.get(False)
                    k += 1
            except queue.Empty:
                # print('Client buffer {} emptied.'.format(i))
                pass

        print('Done.')

    def assemble_dataframe_kwargs(self, time_stamp=None, phasor_data=None, freq_data=None, dfreq_data=None):

        if time_stamp is None:
            time_stamp = time.time()

        # soc = int(time_stamp)
        # frasec = int((((repr((time_stamp % 1))).split("."))[1])[0:6])
        # frasec = int(format(time_stamp % 1, '.6f').split(".")[1])
        soc, frasec = [int(val) for val in format(time_stamp, '.6f').split(".")]    

        data_kwargs = dict(
            soc=soc,
            frasec=(frasec, '+'),
            analog=[],
            digital=[],
            stat=("ok", True, "timestamp", False, False, False, 0, "<10", 0),
        )

        if phasor_data is None:
            if self.n_pmus == 1:
                data_kwargs['phasors'] = [(random.uniform(215.0, 240.0), random.uniform(-np.pi, np.pi)) for _ in range(self.n_phasors)]
            else:
                data_kwargs['phasors'] = [[(random.uniform(215.0, 240.0), random.uniform(-np.pi, np.pi)) for _ in
                               range(n_phasors_per_pmu)] for n_phasors_per_pmu in self.n_phasors_per_pmu]
        else:
            data_kwargs['phasors'] = phasor_data
        
        if freq_data is None:
            data_kwargs['freq'] = 0 if self.n_pmus == 1 else [0]*self.n_pmus
        else:
            data_kwargs['freq'] = freq_data

        if dfreq_data is None:
            data_kwargs['dfreq'] = 0 if self.n_pmus == 1 else [0]*self.n_pmus
        else:
            data_kwargs['dfreq'] = dfreq_data 

        if self.n_pmus > 1:
            for key in ['analog', 'digital', 'stat']:
                data_kwargs[key] = [data_kwargs[key]]*self.n_pmus
        return data_kwargs


    def publish(self, *args, **kwargs):
        data_kwargs = self.assemble_dataframe_kwargs(*args, **kwargs)
        self.pmu.send_data(**data_kwargs)

    def generate_dataframe(self, *args, **kwargs):
        data_kwargs = self.assemble_dataframe_kwargs(*args, **kwargs)
        return self.pmu.generate_dataframe(**data_kwargs)


if __name__ == '__main__':

    publish_frequency = 5
    ip = '10.0.0.39'
    port = 50000

    station_names = ['PMU1', 'PMU2', 'PMU3']
    channel_names = [
        ['Phasor1.1', 'Phasor1.2'],
        ['Phasor2.1', 'Phasor2.2', 'Phasor2.3'],
        ['Phasor3.1'],
    ]

    # station_names = ['PMU1']
    # channel_names = [['Phasor1.1', 'Phasor1.2']]
    pmu = SimplePMU(
        ip, port,
        publish_frequency=publish_frequency,
        station_names=station_names,
        channel_names=channel_names,
    )
    df = pmu.generate_dataframe(freq_data=[50, 51, 50])
    df.get_freq()
    pmu.run()

    k = 0
    while k < 200:
        time.sleep(0.1)
        if pmu.pmu.clients:  # Check if there is any connected PDCs
            k += 1
            print(k)
            # t, v = input_signal

            # Publish C37.118-snapshot
            # pmu_data = [(mag, ang) for mag, ang in zip(np.abs(v), np.angle(v))]
            pmu.publish()

    pmu.cleanup()
    # sys.exit()
    # import threading
    # for thread in threading.enumerate():
    #     print(thread.name)