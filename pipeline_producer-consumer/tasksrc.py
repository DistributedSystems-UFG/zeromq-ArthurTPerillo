import zmq, time, pickle, sys, random
from constPipe import IP_SRC, PORT1, MEASURE_TYPES

#  Faixas de valores por tipo (unidades imperiais) 
RANGES = {
    "temp":   (32.0,   212.0),   # Fahrenheit  (ponto de congelamento→ebulição)
    "weight": (1.0,    500.0),   # Libras
    "dist":   (0.1,    100.0),   # Milhas
    "length": (0.5,    120.0),   # Polegadas
}

LABELS = {
    "temp":   "°F",
    "weight": "lbs",
    "dist":   "mi",
    "length": "in",
}

def generate_measurement() -> dict:
    """Escolhe um tipo aleatório e gera um valor dentro da faixa imperial."""
    mtype       = random.choice(MEASURE_TYPES)
    lo, hi      = RANGES[mtype]
    value       = round(random.uniform(lo, hi), 2)
    return {"type": mtype, "value": value, "unit": LABELS[mtype]}


def run_producer(n_items: int = 0):
    """
    Envia medições pelo socket PUSH.
    n_items == 0  →  produz indefinidamente.
    """
    context = zmq.Context()
    socket  = context.socket(zmq.PUSH)          # socket de envio
    addr    = f"tcp://*:{PORT1}"
    socket.bind(addr)                           # bind na própria máquina

    print(f"[PRODUCER] Ativo em {addr}")
    print(f"[PRODUCER] Tipos: {MEASURE_TYPES}\n")

    count = 0
    try:
        while n_items == 0 or count < n_items:
            m = generate_measurement()
            print(
                f"[PRODUCER] #{count+1:04d}  "
                f"{m['type']:<7} {m['value']:>8.2f} {m['unit']}"
            )
            socket.send(pickle.dumps(m))        # serializa e envia
            time.sleep(random.uniform(0.3, 1.0))  # cadência variável
            count += 1
    except KeyboardInterrupt:
        print("\n[PRODUCER] Interrompido.")
    finally:
        # Sinal de fim para o middleware
        socket.send(pickle.dumps({"type": "STOP", "value": 0, "unit": ""}))
        socket.close()
        context.term()
        print("[PRODUCER] Encerrado.")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    run_producer(n)
