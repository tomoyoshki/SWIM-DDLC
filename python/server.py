import logging
from concurrent.futures import ThreadPoolExecutor

import grpc
import numpy as np

from gopython_pb2 import BatchRequest, BatchResponse
from gopython_pb2_grpc import GoPythonServicer, add_GoPythonServicer_to_server


def find_outliers(data: np.ndarray):
    """Return indices where values more than 2 standard deviations from mean"""
    out = np.where(np.abs(data - data.mean()) > 2 * data.std())
    # np.where returns a tuple for each dimension, we want the 1st element
    return out[0]


class GoPythonServer(GoPythonServicer):
    def SetBatchSize(self, request, context):
        logging.info("Client requesting to set batch size: %s", request.batch_size)
        # Convert metrics to numpy array of values only
        resp = BatchResponse(status="OK")
        return resp


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
    )
    server = grpc.server(ThreadPoolExecutor())
    add_GoPythonServicer_to_server(GoPythonServer(), server)
    port = 9999
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    logging.info('server ready on port %r', port)
    server.wait_for_termination()