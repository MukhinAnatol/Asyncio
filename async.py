import asyncio
from aiohttp import ClientSession
from models import engine, Session, Base, SwapiCharacters
from more_itertools import chunked

CHUNK_SIZE = 20


async def get_people(session, people_id:int):
    async with session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        json_data = await response.json()
        return json_data


async def get_value(links: list):
    async with ClientSession() as session:
        if links:
            requests = [await session.get(link) for link in links]
            responses = [await request.json(content_type=None) for request in requests]
            result = ",".join([response.get('name') or response.get('title') for response in responses])
            return result
        else:
            return None

async def get_planet(link: str):
    async with ClientSession() as session:
        data = await session.get(link)
        planet = await data.json()
        result = planet.get('name')
        return result


async def db_function(results):
    swapichr = [SwapiCharacters(name=item.get('name'),
                                birth_year=item.get('birth_year'),
                                eye_color=item.get('eye_color'),
                                films=await get_value(item.get('films')),
                                gender=item.get('gender'),
                                hair_color=item.get('hair_color'),
                                height=item.get('height'),
                                homeworld=await get_planet(item.get('homeworld')),
                                mass=item.get('mass'),
                                skin_color=item.get('skin_color'),
                                species=await get_value(item.get('species')),
                                starships=await get_value(item.get('starships')),
                                vehicles=await get_value(item.get('vehicles'))) for item in results]
    async with Session() as session:
        session.add_all(swapichr)
        await session.commit()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session = ClientSession()
    coros = (get_people(session, i) for i in range(1, 101))
    for coros_chunk in chunked(coros, CHUNK_SIZE):
        results = await asyncio.gather(*coros_chunk)
        asyncio.create_task(db_function(results))

    set_tasks = asyncio.all_tasks()
    for task in set_tasks:
        if task != asyncio.current_task():
            await task


if __name__ == '__main__':
    asyncio.run(main())