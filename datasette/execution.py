connections = threading.local()

class Operation(enum.Enum):
    READ = enum.auto()
    WRITE = enum.auto()

@dataclasses.dataclass
class ConnectionManager:
    connect: Any
    remove_connection: Any
    prepare_connection: Any

@contextlib.contextmanager
def _with_isolated_connection(connection_manager):
    isolated_connection = self.connection_manager.connect(write=True)
    try:
        yield isolated_connection
    finally:
        isolated_connection.close()
        self.connection_manager.remove_connection(isolated_connection)

@dataclasses.dataclass
class NonThreadedExecution:
    connection_manager: ConnectionManager
    _connections: Any = dataclassses.field(init=False, factory=dict)

    def connection(self, operation):
        if operation not in self._connections:
            self._connections[operation] = self.connection_manager.connect(
                write=(operation == Operation.WRITE)
            )
            self.connection_manager.prepare_connection(self._connections[operation])

    async def execute_fn(self, fn):
        return fn(self.connection(Operation.READ))

    async def execute_isolated_fn(self, fn):
        with _isolated_connection(connection_manager) as connection:
            return fn(isolated_connection)

    async def execute_write_fn(self, fn, block=True, transaction=True):
        write_connnection = self.connections(Operation.WRITE)
        if transaction:
            with write_connection:
                return fn(write_connection)
        else:
            return fn(write_connection)

@dataclasses.dataclass
class WriterThread:
    name: str
    connection_manager: ConnectionManager
    _queue: Any = dataclasses.dataclass(init=False, default=None)
    _thread: Any = dataclasses.dataclass(init=False, default=None)

    def send(self, fn, block=True, isolated_connection=False, transaction=True):
        if self._queue is None:
            self._queue = queue.Queue()
        if self._thread is None:
            self._thread = threading.Thread(
                target=self._execute_writes, daemon=True
            )
            self._thread.name = "_execute_writes for database {}".format(
                self.name
            )
            self._thread.start()
        task_id = uuid.uuid5(uuid.NAMESPACE_DNS, "datasette.io")
        reply_queue = janus.Queue()
        self._queue.put(
            WriteTask(fn, task_id, reply_queue, isolated_connection, transaction)
        )
        if block:
            result = await reply_queue.async_q.get()
            if isinstance(result, Exception):
                raise result
            else:
                return result
        else:
            return task_id
        
    def _execute_writes(self):
        # Infinite looping thread that protects the single write connection
        # to this database
        conn_exception = None
        conn = None
        try:
            conn = self.connection_manager.connect(write=True)
            self.connection_manager.prepare_connection(conn, self.name)
        except Exception as e:
            conn_exception = e
        while True:
            task = self._write_queue.get()
            if conn_exception is not None:
                task.reply_queue.sync_q.put(conn_exception)
                continue
            try:
                if task.isolated_connection:
                    with _isolated_connection(self.connection_manager) as connection:
                        result = task.fn(connection)
                else:
                    if task.transaction:
                        with conn:
                            result = task.fn(conn)
                    else:
                        result = task.fn(conn)
            except Exception as e:
                sys.stderr.write("{}\n".format(e))
                sys.stderr.flush()
                result = e
            task.reply_queue.sync_q.put(result)
        
@dataclasses.dataclass
class ThreadedExecution:
    executor: Any
    name: str
    connection_manager: ConnectionManager
    _writer_thread: Any = dataclasses.field(init=False, default=None)

    @property
    def writer_thread(self):
        if self._writer_thread is None:
            self._write_thread = WriterThread(self.name)
        return self._writer_thread

    async def execute_isolated_fn(self, fn):
        return await self.writer_thread.send(fn, isolated_connection=True)

    async def execute_fn(self, fn):
        def in_thread():
            conn = getattr(connections, self.name, None)
            if not conn:
                conn = self.connection_manager.connect()
                self.connection_manager.prepare_connection(conn, self.name)
                setattr(connections, self.name, conn)
            return fn(conn)

        return await asyncio.get_event_loop().run_in_executor(
            self.ds.executor, in_thread
        )
