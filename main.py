import asyncio
from typing import Coroutine

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, PydanticUserError

from database import DbAccounts
from binance_main import binance_main


app = FastAPI()


# noinspection PyTypeChecker
app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:3000'],
    allow_methods=['*'],
    allow_headers=['*']
)


class Test(BaseModel):

    a: str


@app.get("/core/otR/CM")
async def root_cm():
    return {'message': "It's project by Akkerman."}


@app.get("/core/otR/CM/return_account_data")
async def return_account_data():
    return await DbAccounts().return_data()


@app.get("/core/otR/CM/start_bot")
async def cm_start_bot():
    try:
        Test.a = asyncio.current_task(asyncio.get_running_loop()).get_coro()
        await asyncio.create_task(binance_main())
    except asyncio.exceptions.CancelledError:
        return {'bot': 'off'}


@app.get("/core/otR/CM/close_bot")
async def cm_close_bot():

    for task in asyncio.all_tasks():
        if Test.a == task.get_coro():
            response_task = task
    try:
        response_task.cancel()
    except:
        return {'status': 'error'}

    return {'status': 'closed'}


@app.post('/post_test')
async def post_test(t: Test):
    return t
