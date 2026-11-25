import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


produtos = [
    {
        "produto_id": 1,
        "produto": "Notebook",
        "preco": 3500.00,
    },
    {
        "produto_id": 2,
        "produto": "Mouse",
        "preco": 120.00,
    },
    {
        "produto_id": 3,
        "produto": "Teclado",
        "preco": 250.00,
    },
    {
        "produto_id": 4,
        "produto": "Monitor",
        "preco": 1200.00,
    },
]


def gerar_pedido():

    produto = random.choice(produtos)

    quantidade = random.randint(1, 12)

    # Simula pedidos com quantidade inválida
    if random.random() < 0.20:
        quantidade = 0

    valor_total = produto["preco"] * quantidade

    pedido = {
        "pedido_id": random.randint(1, 1000000),
        "produto_id": produto["produto_id"],
        "produto": produto["produto"],
        "quantidade": quantidade,
        "valor_total": valor_total,
        "timestamp": datetime.now().isoformat()
    }

    return pedido


try:

    while True:

        pedido = gerar_pedido()

        producer.send("pedidos", pedido)

        producer.flush()

        print(f"Pedido enviado: {pedido}")

        time.sleep(2)

except KeyboardInterrupt:

    print("\nProducer finalizado pelo usuário.")

finally:

    producer.close()
