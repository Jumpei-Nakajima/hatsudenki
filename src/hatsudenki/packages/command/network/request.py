import json

import aiohttp


async def aio_http_post_json(uri: str, body: dict, headers: dict):
    """
    postを投げてjsonでresponse取得

    :param uri: リクエスト先URL
    :param body: リクエストボディ
    :param headers: ヘッダ
    :return: レスポンス
    """
    async with aiohttp.ClientSession() as s:
        async with s.post(uri, data=body, headers=headers) as resp:
            return json.loads(await resp.read())


async def aio_http_put_json(uri: str, body: dict, headers: dict):
    """
    putを投げてjsonでresponse取得

    :param uri: リクエスト先URL
    :param body: リクエストボディ
    :param headers: ヘッダ
    :return: レスポンス
    """
    async with aiohttp.ClientSession() as s:
        async with s.put(uri, data=body, headers=headers) as resp:
            if resp.status != 200:
                raise Exception(await resp.read())
            return json.loads(await resp.read())


async def aio_http_get_json(uri: str, headers: dict):
    """
    getを投げてjsonでresponse取得

    :param uri: リクエスト先URL
    :param headers: ヘッダ
    :return: レスポンス
    """
    async with aiohttp.ClientSession() as s:
        async with s.get(uri, headers=headers) as resp:
            if resp.status != 200:
                raise Exception('status is not 200!!!')
            return json.loads(await resp.read())
