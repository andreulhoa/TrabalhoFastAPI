from automata.tm.dtm import DTM
from fastapi import Depends
from fastapi_mail import ConnectionConfig, MessageSchema, MessageType, FastMail
from sql_app import crud, models, schemas
from sql_app.database import engine, SessionLocal
from sqlalchemy.orm import Session

from util.email_body import EmailSchema

from aio_pika import connect, IncomingMessage

import asyncio
import json


conf = ConnectionConfig(
    MAIL_USERNAME="d1edc21b4b70fb",
    MAIL_PASSWORD="1fed68f7cb8510",
    MAIL_FROM="andre.werneck@estudante.ufla.br",
    MAIL_PORT=587,
    MAIL_SERVER="sandbox.smtp.mailtrap.io",
    MAIL_STARTTLS=False,
    MAIL_SSL_TLS=False,
    USE_CREDENTIALS=True,
    VALIDATE_CERTS=True
)

db = SessionLocal()

async def on_message(message: IncomingMessage):
    txt = message.body.decode("utf-8")
    print(json.loads(txt))

# Verificação de estados, simbolos, e transições (REGRAS DA MAQUINA)
async def on_message(message: IncomingMessage):
    info = json.loads(message.body.decode("utf-8"))
    states = set(info.get("states", []))
    print("states2", info)

    if len(states) == 0:
        result = "rejected: states cannot be empty"

    input_symbols = set(info.get("input_symbols", []))
    if len(input_symbols) == 0:
        result = "rejected: input_symbols cannot be empty"
        
    tape_symbols = set(info.get("tape_symbols", []))
    if len(tape_symbols) == 0:
        result = "rejected: tape_symbols cannot be empty"

    initial_state = info.get("initial_state", "")
    if initial_state == "":
        result = "rejected: initial_state cannot be empty"
        
    blank_symbol = info.get("blank_symbol", "")
    if blank_symbol == "":
        result = "rejected: blank_symbol cannot be empty"
        
    final_states = set(info.get("final_states", []))
    if len(final_states) == 0:
        result = "rejected: final_states cannot be empty"
        
    transitions = dict(info.get("transitions", {}))
    if len(transitions) == 0:
        result = "rejected: transitions cannot be empty"

    input = info.get("input", "")
    if input == "":
        result = "rejected: input cannot be empty"

    dtm = DTM(
        states=states,
        input_symbols=input_symbols,
        tape_symbols=tape_symbols,
        transitions=transitions,
        initial_state=initial_state,
        blank_symbol=blank_symbol,
        final_states=final_states,
    )
    if dtm.accepts_input(input):
        result = "accepted"
    else:
        result = "rejected"

    history = schemas.History(query=str(info), result=result)
    crud.create_history(db=db, history=history)

    email_shema = EmailSchema(email=["to@example.com"])

    await simple_send(email_shema, result=result, configuration=str(info))

# LOOP de processamento
async def main(loop):
    # try:
    connection = await connect("amqp://guest:guest@rabbitmq:5672/", loop = loop)

    channel = await connection.channel()

    queue = await channel.declare_queue("dtm_queue")       

    await queue.consume(on_message, no_ack = True)
    pass
    # except Exception as e:
    #     print("ih deu ruim", e)


# Envio do email
async def simple_send(email: EmailSchema, result: str, configuration: str):
    html = """
    <p>Thanks for using Fastapi-mail</p>
    <p> The result is: """ + result + """</p>
    <p> We have used this configuration: """ + configuration + """</p>
    """
    message = MessageSchema(
        subject="Fastapi-Mail module",
        recipients=email.dict().get("email"),
        body=html,
        subtype=MessageType.html)

    fm = FastMail(conf)
    await fm.send_message(message)
    return "OK"


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main(loop))
    loop.run_forever()