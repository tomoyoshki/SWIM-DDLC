import os
import logging
import grpc
import json
import numpy as np
from pathlib import Path
import signal
import shutil
import torch
from torchvision import datasets, models, transforms
import warnings


from concurrent.futures import ThreadPoolExecutor
from gopython_pb2 import InitializeRequest, InitializeResponse, InferenceRequest, InferenceResponse, RemoveResponse
from gopython_pb2_grpc import GoPythonServicer, add_GoPythonServicer_to_server

warnings.filterwarnings('ignore')


server = None

model1 = None
model1_initialized = False
model1_batch_size = 1
model1_name = ""

model2 = None
model2_initialized = False
model2_batch_size = 1
model2_name = ""

image_utils = None

job1_done = True
job2_done = True

model_types = ["", ""]


def SigINTHandler(signum, frame):
    if server != None:
        logging.info("Received control-c, now stopping model")
        server.stop(None)

def prepareModel(job_id, batch_size, model_type, model_name="resnet50"):
    global model1
    global model2
    global model1_initialized
    global model2_initialized
    global model1_name
    global model2_name

    logging.info(f"Preparing model {model_name} with type {model_type} for job {job_id} ")
    logging.info(f"Model 1 initialized: {model1_initialized}")
    logging.info(f"Model 2 initialized: {model2_initialized}")
    if job_id == 0:
        if model1_initialized == False:
            logging.info(f"model1 named {model_name} is initialized")
            if model_type == "image":

                if model_name == "resnet50":
                    model1 = models.resnet50(pretrained=False)
                    model1.load_state_dict(torch.load("./python/resnet50.pth"))
                elif model_name == "resnet18":
                    model1 = models.resnet18(pretrained=False)
                    model1.load_state_dict(torch.load("./python/resnet18.pth"))
                else:
                    model1 = models.resnet50(pretrained=False)
                    model1.load_state_dict(torch.load("./python/resnet50.pth"))
            elif model_type == "speech":
                model1 = torch.hub.load('pytorch/fairseq', 'transformer.wmt14.en-fr', tokenizer='moses', bpe='subword_nmt')
            else:
                logging.info("Received bad model type")
                return -1
            model1.eval()
            model1_initialized = True
            model1_name = model_name
        else:
            logging.info("Model1 is currently occupied")
            return -2
    elif job_id == 1:
        if model2_initialized == False:
            logging.info(f"Model2 named {model_name} is initialized")
            if model_type == "image":
                if model_name == "resnet50":
                    model2 = models.resnet50(pretrained=False)
                    model2.load_state_dict(torch.load("./python/resnet50.pth"))
                elif model_name == "resnet18":
                    model2 = models.resnet18(pretrained=False)
                    model2.load_state_dict(torch.load("./python/resnet18.pth"))
                else:
                    model2 = models.resnet50(pretrained=False)
                    model2.load_state_dict(torch.load("./python/resnet50.pth"))
            elif model_type == "speech":
                model2 = torch.hub.load('pytorch/fairseq', 'transformer.wmt14.en-fr', tokenizer='moses', bpe='subword_nmt')
            else:
                logging.info("Received bad model type")
                return -1
            model2.eval()
            model2_initialized = True
            model2_name = model_name
        else:
            logging.info("Model2 is currently occupied")
            return -2
    else:
        logging.info("Invalid job id, we only support job0 and job1")
        return -2
    model_types[job_id] = model_type
    return 0

def processData(job_id, batch_id, data_folder):
    if model_types[job_id] == "image":
        logging.info("Image inferencing")
        data_transform = transforms.Compose([transforms.ToTensor()])
        image_datasets = datasets.ImageFolder(data_folder, data_transform)
        job_batch_size = model1_batch_size
        if job_id == 1:
            job_batch_size = model2_batch_size
        dataloader = torch.utils.data.DataLoader(image_datasets, batch_size=job_batch_size)
        with open("./python/imagenet_classes.txt", "r") as f:
            categories = [s.strip() for s in f.readlines()]
        inference_result = {}
        with torch.no_grad():
            for i, (images, _) in enumerate(dataloader, 0):
                if job_id == 0:
                    res = model1(images)
                else:
                    res = model2(images)
                output = torch.nn.functional.softmax(res, dim=1)[0]
                top5_prob, top5_catid = torch.topk(output, 5)
                sample_fname, _ = dataloader.dataset.samples[i]
                inference_result[sample_fname] = [categories[top5_catid[i]] for i in range(5)]
        return inference_result
    else:
        logging.info("Speech inferencing")

def checkfile_validity(dir, create=False):
    if os.path.exists(dir):
        shutil.rmtree(dir)
    if create:
        os.makedirs(dir)

    
class GoPythonServer(GoPythonServicer):
    def InitializeModel(self, request, context):
        logging.info("Master requesting to intialize model")
        resp = InitializeResponse(status="OK")
        model_type = request.model_type
        res = prepareModel(request.job_id, request.batch_size, model_type, request.model_name)
        if res == -1:
            resp.status = "Error"
        elif res == -2:
            resp.status = "No model available"
        return resp
    
    def RemoveModel(self, request, context):
        logging.info("Master requesting on removing model")
        resp = RemoveResponse(status="OK")
        if request.job_id == 0:
            global model1
            global model1_initialized
            global model1_batch_size
            global job1_done
            global model1_name

            del model1
            model1_initialized = False
            model1_batch_size = False
            job1_done = True
            model1_name = False

        elif request.job_id == 1:
            global model2
            global model2_initialized
            global model2_batch_size
            global job2_done
            global model2_name

            del model2
            model2_initialized = False
            model2_batch_size = False
            job2_done = True
            model2_name = False
        else:
            logging.info("Invalid model")

        checkfile_validity(f'./python/data/{request.job_id}')
        shutil.rmtree(f'./python/result/{request.job_id}')


    
    def ModelInference(self, request, context):
        logging.info("Inference on data")
        job_id = request.job_id
        batch_id = request.batch_id
        inference_size = request.inference_size
        inference_data_folder = request.inference_data_folder
        result = processData(job_id, batch_id, inference_data_folder)

        if result is None:
            raise Exception("processData did not return any inference result")

        logging.info(f"Batchid: {batch_id}")
        result_directory = f"./python/result/{job_id}/{batch_id}/"

        # checkfile_validity(result_directory, True)
        if not os.path.exists(result_directory):
            os.makedirs(result_directory)

        with open(result_directory + "result.txt", "w") as f:
            for input_filename in result:
                f.write("%s: %s\n" % (input_filename, result[input_filename]))

        res = json.dumps(result).encode('utf-8')
        resp = InferenceResponse(status="OK", inference_result=res)
        return resp



if __name__ == '__main__':
    if os.path.exists("./python_log.log"):
        os.remove("./python_log.log")
    

    checkfile_validity("./python/data")
    checkfile_validity("./python/result")
    checkfile_validity("./python/results")

    logging.basicConfig(
        filename="./python_log.log",
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
    )

    logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)

    signal.signal(signal.SIGINT, SigINTHandler)

    server = grpc.server(ThreadPoolExecutor())
    add_GoPythonServicer_to_server(GoPythonServer(), server)
    port = 9999
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    logging.info('server ready on port %r', port)
    server.wait_for_termination()