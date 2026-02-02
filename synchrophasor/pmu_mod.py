import logging
from multiprocessing import Event, Process, Queue
from select import select
from sys import stdout
from time import sleep, time

from synchrophasor.frame import DataFrame, CommandFrame, CommonFrame, FrameError
from synchrophasor.pmu import Pmu, PmuError


class PmuMod(Pmu):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stopped = False
        self.client_stop_events = []

    def stop(self):
        self._stopped = True

    def generate_dataframe(self, phasors=[], analog=[], digital=[], freq=0, dfreq=0,
                  stat=("ok", True, "timestamp", False, False, False, 0, "<10", 0), soc=None, frasec=None):

        # PH_UNIT conversion
        if phasors and self.cfg2.get_num_pmu() > 1:  # Check if multistreaming:
            if not (self.cfg2.get_num_pmu() == len(self.cfg2.get_data_format()) == len(phasors)):
                raise PmuError("Incorrect input. Please provide PHASORS as list of lists with NUM_PMU elements.")

            for i, df in enumerate(self.cfg2.get_data_format()):
                if not df[1]:  # Check if phasor representation is integer
                    phasors[i] = map(lambda x: int(x / (0.00001 * self.cfg2.get_ph_units()[i])), phasors[i])
        elif not self.cfg2.get_data_format()[1]:
            phasors = map(lambda x: int(x / (0.00001 * self.cfg2.get_ph_units())), phasors)

        # AN_UNIT conversion
        if analog and self.cfg2.get_num_pmu() > 1:  # Check if multistreaming:
            if not (self.cfg2.get_num_pmu() == len(self.cfg2.get_data_format()) == len(analog)):
                raise PmuError("Incorrect input. Please provide analog ANALOG as list of lists with NUM_PMU elements.")

            for i, df in enumerate(self.cfg2.get_data_format()):
                if not df[2]:  # Check if analog representation is integer
                    analog[i] = map(lambda x: int(x / self.cfg2.get_analog_units()[i]), analog[i])
        elif not self.cfg2.get_data_format()[2]:
            analog = map(lambda x: int(x / self.cfg2.get_analog_units()), analog)

        data_frame = DataFrame(self.cfg2.get_id_code(), stat, phasors, freq, dfreq, analog, digital, self.cfg2, soc, frasec)
        return data_frame
    
    def send_data(self, *args, **kwargs):
        
        data_frame = self.generate_dataframe(*args, **kwargs)
        for buffer in self.client_buffers:
            buffer.put(data_frame)

    def acceptor(self):

        while True:
            # print('Acceptor running')
            self.logger.info("[%d] - Waiting for connection on %s:%d", self.cfg2.get_id_code(), self.ip, self.port)

            # Accept a connection on the bound socket and fork a child process to handle it.
            # print('Waiting for socket.accept')
            conn, address = self.socket.accept()
            if self._stopped:
                conn.close()
                break
            # print('Not waiting for socket.accept')

            # Create Queue which will represent buffer for specific client and add it o list of all client buffers
            buffer = Queue()
            self.client_buffers.append(buffer)

            pdc_handler_data_rate = 5*self.cfg2.get_data_rate()
            stop_event = Event()
            process = Process(target=self.pdc_handler, args=(conn, address, buffer, self.cfg2.get_id_code(),
                                                             pdc_handler_data_rate, self.cfg1, self.cfg2,
                                                             self.cfg3, self.header, self.buffer_size,
                                                             self.set_timestamp, self.logger.level, stop_event))

            process.daemon = True

            # for i, thread in enumerate(threading.enumerate()):
            #     print(thread)

            process.start()

            # for i, thread in enumerate(threading.enumerate()):
            #     print(thread)
            self.clients.append(process)
            self.client_stop_events.append(stop_event)

            # Close the connection fd in the parent, since the child process has its own reference.
            conn.close()

    @staticmethod
    def pdc_handler(connection, address, buffer, pmu_id, data_rate, cfg1, cfg2, cfg3, header,
                    buffer_size, set_timestamp, log_level, stop_event):

        # Recreate Logger (handler implemented as static method due to Windows process spawning issues)
        logger = logging.getLogger(address[0] + str(address[1]))
        logger.setLevel(log_level)
        handler = logging.StreamHandler(stdout)
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        logger.info("[%d] - Connection from %s:%d", pmu_id, address[0], address[1])

        # Wait for start command from connected PDC/PMU to start sending
        sending_measurements_enabled = False

        # Calculate delay between recorded_pmu_data_raw frames
        if data_rate > 0:
            delay = 1.0 / data_rate
        else:
            delay = -data_rate

        try:
            while not stop_event.is_set():
                # print('Looping')

                command = None
                received_data = b""
                readable, writable, exceptional = select([connection], [], [], 0)  # Check for client commands
                # print('Check 1')

                if readable:
                    """
                    Keep receiving until SYNC + FRAMESIZE is received, 4 bytes in total.
                    Should get this in first iteration. FRAMESIZE is needed to determine when one complete message
                    has been received.
                    """
                    # print('Check 1-2')
                    while len(received_data) < 4 and not stop_event.is_set():
                        # print('Check 2')
                        received_data += connection.recv(1)  # buffer_size)
                        # print('Check 3')
                    # print('Check 4')
                    bytes_received = len(received_data)
                    total_frame_size = int.from_bytes(received_data[2:4], byteorder="big", signed=False)

                    # Keep receiving until every byte of that message is received
                    while bytes_received < total_frame_size and not stop_event.is_set():
                        message_chunk = connection.recv(min(total_frame_size - bytes_received, buffer_size))
                        if not message_chunk:
                            break
                        received_data += message_chunk
                        bytes_received += len(message_chunk)

                    # If complete message is received try to decode it
                    if len(received_data) == total_frame_size:
                        try:
                            received_message = CommonFrame.convert2frame(received_data)  # Try to decode received recorded_pmu_data_raw

                            if isinstance(received_message, CommandFrame):
                                command = received_message.get_command()
                                logger.info("[%d] - Received command: [%s] <- (%s:%d)", pmu_id, command,
                                            address[0], address[1])
                            else:
                                logger.info("[%d] - Received [%s] <- (%s:%d)", pmu_id,
                                            type(received_message).__name__, address[0], address[1])
                        except FrameError:
                            logger.warning("[%d] - Received unknown message <- (%s:%d)", pmu_id, address[0], address[1])
                    else:
                        logger.warning("[%d] - Message not received completely <- (%s:%d)", pmu_id, address[0],
                                       address[1])

                if command:
                    if command == "start":
                        sending_measurements_enabled = True
                        logger.info("[%d] - Start sending -> (%s:%d)", pmu_id, address[0], address[1])

                    elif command == "stop":
                        logger.info("[%d] - Stop sending -> (%s:%d)", pmu_id, address[0], address[1])
                        sending_measurements_enabled = False

                    elif command == "header":
                        if set_timestamp: header.set_time()
                        connection.sendall(header.convert2bytes())
                        logger.info("[%d] - Requested Header frame sent -> (%s:%d)",
                                    pmu_id, address[0], address[1])

                    elif command == "cfg1":
                        if set_timestamp: cfg1.set_time()
                        connection.sendall(cfg1.convert2bytes())
                        logger.info("[%d] - Requested Configuration frame 1 sent -> (%s:%d)",
                                    pmu_id, address[0], address[1])

                    elif command == "cfg2":
                        if set_timestamp: cfg2.set_time()
                        connection.sendall(cfg2.convert2bytes())
                        logger.info("[%d] - Requested Configuration frame 2 sent -> (%s:%d)",
                                    pmu_id, address[0], address[1])

                    elif command == "cfg3":
                        if set_timestamp: cfg3.set_time()
                        connection.sendall(cfg3.convert2bytes())
                        logger.info("[%d] - Requested Configuration frame 3 sent -> (%s:%d)",
                                    pmu_id, address[0], address[1])

                if sending_measurements_enabled and not buffer.empty():

                    data = buffer.get()
                    if isinstance(data, CommonFrame):  # If not raw bytes convert to bytes
                        if set_timestamp: data.set_time()
                        data = data.convert2bytes()

                    sleep(delay)
                    connection.sendall(data)
                    logger.debug("[%d] - Message sent at [%f] -> (%s:%d)",
                                 pmu_id, time(), address[0], address[1])

            # print('Client loop finished')
        except Exception as e:
            print(e)
        finally:
            connection.close()
            logger.info("[%d] - Connection from %s:%d has been closed.", pmu_id, address[0], address[1])
