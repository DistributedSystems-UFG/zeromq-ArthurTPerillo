"""
Publica leituras de sensores em unidades imperiais em múltiplos tópicos.
Cada subscriber escolhe quais tópicos quer receber.

Tópicos publicados:
  TEMP    → temperatura em Fahrenheit
  WEIGHT  → peso em Libras
  DIST    → distância em Milhas
  LENGTH  → comprimento em Polegadas

Uso:
  python zmq_publisher.py [porta]     (padrão: 12345)
"""

import zmq, time, random, sys

# ── Configuração de sensores ─────────────────────────────────────────────────

SENSORS = {
    #  tópico   : (rótulo,        unidade, faixa_min, faixa_max)
    "TEMP"   : ("Temperatura",  "°F",   -40.0,  120.0),
    "WEIGHT" : ("Peso",         "lbs",    1.0,  500.0),
    "DIST"   : ("Distância",    "mi",     0.1,  100.0),
    "LENGTH" : ("Comprimento",  "in",     0.5,  120.0),
}

def read_sensor(topic: str) -> float:
    """Simula a leitura de um sensor com variação aleatória."""
    _, _, lo, hi = SENSORS[topic]
    return round(random.uniform(lo, hi), 2)


def run_publisher(port: int = 5678):
    context = zmq.Context()
    socket  = context.socket(zmq.PUB)       # socket publicador
    socket.bind(f"tcp://*:{port}")          # bind em todas as interfaces

    print(f"[PUB] Publicando na porta {port}")
    print(f"[PUB] Tópicos: {list(SENSORS.keys())}")
    print(f"[PUB] Intervalo: ~2s por ciclo de tópicos\n")

    topics = list(SENSORS.keys())
    seq    = 0                              # número de sequência global

    try:
        while True:
            # Publica um tópico por vez em sequência
            topic = topics[seq % len(topics)]
            label, unit, *_ = SENSORS[topic]
            value = read_sensor(topic)

            # Formato da mensagem: "TOPICO valor unidade timestamp"
            ts      = time.strftime("%H:%M:%S")
            payload = f"{topic} {value:.2f} {unit} {ts}"
            socket.send(payload.encode())

            print(f"[PUB] {payload}")
            seq   += 1
            time.sleep(2)                   # 2 s entre publicações

    except KeyboardInterrupt:
        print("\n[PUB] Encerrado.")
    finally:
        socket.close()
        context.term()


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5678
    run_publisher(port)
