import threading
import boto3
from werkzeug.serving import make_server, WSGIRequestHandler
from werkzeug.urls import uri_to_iri
from moto.server import DomainDispatcherApplication, create_backend_app


class S3SimulatorRequestHandler(WSGIRequestHandler):
    """Custom request handler with logging for S3 requests."""

    bucket_name = None

    def log_request(self, code='-', size='-'):
        """Log S3 requests with range headers."""
        path = uri_to_iri(self.path)
        if self.bucket_name and self.bucket_name in path:
            range_header = self.headers.get('Range', 'No range')
            code = str(code)
            print('📡 %06s [%24s] %s %s %s ' % (self.command, range_header, path, code, size))


class S3Simulator:
    """Simulates an S3 object store using moto for testing purposes."""

    def __init__(self, bucket_name="data-lake", port=5000):
        self.bucket_name = bucket_name
        self.port = port
        self.server = None
        self.thread = None

    def start(self):
        """Start the S3 simulator server."""
        if self.server is not None:
            raise RuntimeError("S3 server is already running")

        # Configure the custom request handler with bucket name
        S3SimulatorRequestHandler.bucket_name = self.bucket_name

        # Create the WSGI application
        app = DomainDispatcherApplication(create_backend_app)

        # Create and start the server
        self.server = make_server(
            "127.0.0.1",
            self.port,
            app,
            request_handler=S3SimulatorRequestHandler
        )

        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.daemon = True
        self.thread.start()

        # Create the bucket
        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://127.0.0.1:{self.port}",
            aws_access_key_id="fake",
            aws_secret_access_key="fake",
            region_name="us-east-1"
        )
        s3.create_bucket(Bucket=self.bucket_name)
        print(f"S3 Server running on port {self.port}")

    def stop(self):
        """Stop the S3 simulator server."""
        if self.server is not None:
            self.server.shutdown()
            self.server = None
            self.thread = None
            S3SimulatorRequestHandler.bucket_name = None
