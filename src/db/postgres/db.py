from typing import Iterable

from aiopg import connect

from ..abc import DB
from ...amo.schemas import DBStatePurchase
from ...settings import settings


class PostgresDB(DB):
    async def write_purchases(self, purchases: Iterable[DBStatePurchase]) -> None:
        print("writing data to DB...")
        for purchase in purchases:
            await self._write_purchase(purchase)
        print("DONE!")

    async def _write_purchase(self, purchase: DBStatePurchase) -> None:
        stmt = """
                INSERT INTO state_purchases (
                eis_url,
                winner_name,
                inn,
                result_date,
                customer_name,
                nmck,
                contract_securing,
                warranty_obligations_securing,
                contract_end_date,
                winner_price,
                phone_1,
                fio_1,
                email_1,
                phone_2,
                fio_2,
                email_2,
                phone_3,
                fio_3,
                email_3,
                smp_advantages,
                smp_status,
                extraction_dt,
                purchase_number
            ) VALUES (
                %(eis_url)s,
                %(winner_name)s,
                %(inn)s,
                %(result_date)s,
                %(customer_name)s,
                %(nmck)s,
                %(contract_securing)s,
                %(warranty_obligations_securing)s,
                %(contract_end_date)s,
                %(winner_price)s,
                %(phone_1)s,
                %(fio_1)s,
                %(email_1)s,
                %(phone_2)s,
                %(fio_2)s,
                %(email_2)s,
                %(phone_3)s,
                %(fio_3)s,
                %(email_3)s,
                %(smp_advantages)s,
                %(smp_status)s,
                %(extraction_dt)s,
                %(purchase_number)s
            )
            ON CONFLICT (purchase_number) DO UPDATE SET
                eis_url = EXCLUDED.eis_url,
                winner_name = EXCLUDED.winner_name,
                inn = EXCLUDED.inn,
                result_date = EXCLUDED.result_date,
                customer_name = EXCLUDED.customer_name,
                nmck = EXCLUDED.nmck,
                contract_securing = EXCLUDED.contract_securing,
                warranty_obligations_securing = EXCLUDED.warranty_obligations_securing,
                contract_end_date = EXCLUDED.contract_end_date,
                winner_price = EXCLUDED.winner_price,
                phone_1 = EXCLUDED.phone_1,
                fio_1 = EXCLUDED.fio_1,
                email_1 = EXCLUDED.email_1,
                phone_2 = EXCLUDED.phone_2,
                fio_2 = EXCLUDED.fio_2,
                email_2 = EXCLUDED.email_2,
                phone_3 = EXCLUDED.phone_3,
                fio_3 = EXCLUDED.fio_3,
                email_3 = EXCLUDED.email_3,
                smp_advantages = EXCLUDED.smp_advantages,
                smp_status = EXCLUDED.smp_status,
                extraction_dt = EXCLUDED.extraction_dt
                """
        await self._execute_statement(stmt, **purchase.model_dump(by_alias=False))

    async def _execute_statement(self, stmt: str, **kwargs) -> None:
        async with self._conn.cursor() as cursor:
            await cursor.execute(stmt, kwargs)

    async def _get_connection(self):
        return await connect(
            host=settings.db_host,
            port=settings.db_port,
            database=settings.db_name,
            user=settings.db_user,
            password=settings.db_password,
        )

    async def _close_connection(self):
        await self._conn.close()
