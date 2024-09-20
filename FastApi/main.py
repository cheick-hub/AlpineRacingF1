import logging.handlers
import colorlog
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import logging
import traceback

logger = logging.getLogger('main_log')
logger.setLevel(logging.INFO)

text_format = '%(log_color)s%(asctime)s | %(levelname)s | %(lineno)d | %(message)s '
file_format = '%(asctime)s | %(levelname)s | %(lineno)d | %(message)s '

file_handler = logging.handlers.TimedRotatingFileHandler('log.log',
                                                         when='midnight',
                                                         backupCount=14)
file_handler.setFormatter(logging.Formatter(file_format))
file_handler.setLevel(logging.DEBUG)
logger.addHandler(file_handler)

log_info_console = logging.StreamHandler()
log_info_console.setLevel(logging.DEBUG)
log_info_console.setFormatter(colorlog.ColoredFormatter(text_format))
logger.addHandler(log_info_console)


logger.info('Starting FastAPI')

from Analytics import applications
from modules import run, catana, fiav6#, push
from Analytics.applications import ANALYTICS
app = FastAPI(
    responses={404: {"description": "Not found"}}
)
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# app.include_router(push.router)
app.include_router(catana.router)
app.include_router(run.router)
app.include_router(applications.router)
app.include_router(fiav6.router)

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request,
                                       exc: RequestValidationError):
    """
        Function used to catch request error such as Unprocessable Entity.
    """
    exc_str = f'{exc}'.replace('\n', ' ').replace('   ', ' ')
    exc_msg = exc.errors()[0]
    if len(exc_msg['input']) > 500:   # to keep only 'small' inputs
        exc_msg.pop('input')
    func = request.url.path
    logger.error(f" {request.client.host} | {func} | {exc_msg}")
    content = {'status_code': 10422, 'message': exc_str, 'data': None}
    return JSONResponse(content=content,
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)

@app.exception_handler(Exception)
async def exception_handler(request: Request,
                            exc: Exception):
    """
        Function used to catch any Exception that could happen.
    """
    # exc_str = f'{exc}'.replace('\n', ' ').replace('   ', ' ')
    last_tb = traceback.format_tb(exc.__traceback__)[-10:]
    last_tb = list(map(lambda x : x.split('\n')[:2],last_tb))
    last_tb = [[e.lstrip() for e in tb] for tb in last_tb][::-1]

    exc_msg = f"{exc.__class__.__name__} : {str(exc.args)}\n"
    i = 0
    stop = False
    while i < len(last_tb) and not stop:
        tb_msg = last_tb[i]
        if tb_msg[0].endswith('wrapper'):
            stop = True
        else:
            exc_msg = f"{exc_msg}\t Error in {tb_msg[0]} at {tb_msg[1]}\n"
            i += 1
    func = request.url.path
    logger.error(f" {request.client.host} | {func} | {exc_msg}")
    content = {'status_code': 10500, 'message': exc_msg, 'data': None}
    return JSONResponse(content=content,
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

@app.get("/")
async def root(request: Request):
    logger.debug(f' Ping from {request.client.host}')
    return {"message": "Documentation is available at /docs"}

logger.info('FastAPI Started')

if __name__ == '__main__':
    print("test")