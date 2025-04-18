import logging
from fastapi import FastAPI
import uvicorn


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


app = FastAPI(
    title='Import timeline',
    description='Вебхук для передачи таймлайна сделок с одного портала на другой'
)


@app.get('/ping', status_code=200, tags=['Main'])
async def ping():
    return {'Message': 'Pong'}


# @app.post('/import', status_code=200, tags=['Main'])
# async def import_timeline(deal_id: int) -> dict:
#     message = f"Received webhook for deal ID: {deal_id}"
#     logger.info(message)
#     result = await get_activities(deal_id)
#     logger.info(json.dumps(result))
#     return result


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=11002)
