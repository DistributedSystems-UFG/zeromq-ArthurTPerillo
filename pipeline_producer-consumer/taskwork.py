import zmq, time, pickle, sys, threading
from constPipe import IP_SRC, IP_MID, PORT1, PORT2

#  Funções de conversão Imperial → SI 

CONVERSIONS = {
    #  tipo     : (função,                    unidade_entrada, unidade_saída)
    "temp"   : (lambda f: (f - 32) * 5 / 9,  "°F",  "°C" ),
    "weight" : (lambda x: x * 0.453592,       "lbs", "kg" ),
    "dist"   : (lambda x: x * 1.60934,        "mi",  "km" ),
    "length" : (lambda x: x * 2.54,           "in",  "cm" ),
}

def convert(measurement: dict) -> dict:
    """
    Recebe um dicionário imperial e retorna um dicionário com o valor
    convertido para SI, adicionando metadados de rastreamento.
    """
    mtype = measurement["type"]
    if mtype not in CONVERSIONS:
        raise ValueError(f"Tipo desconhecido: {mtype}")

    fn, unit_in, unit_out = CONVERSIONS[mtype]
    result = round(fn(measurement["value"]), 4)

    return {
        "type"      : mtype,
        "original"  : measurement["value"],
        "unit_in"   : unit_in,
        "converted" : result,
        "unit_out"  : unit_out,
        "worker"    : measurement.get("worker", "?"),  # id do worker
    }


# ── Worker de conversão (thread interna) ─────────────────────────────────────

def conversion_worker(worker_id: int, context: zmq.Context, stop_event: threading.Event):
    """
    Thread worker: recebe tarefas do socket interno DEALER,
    converte e devolve ao sink via socket interno DEALER.
    """
    receiver = context.socket(zmq.PULL)
    receiver.connect("inproc://tasks")          # recebe do distribuidor interno

    sender = context.socket(zmq.PUSH)
    sender.connect("inproc://results")          # envia ao coletor interno

    wid = f"MID-W{worker_id:02d}"
    print(f"[{wid}] Worker de conversão iniciado.")

    while not stop_event.is_set():
        try:
            if not receiver.poll(500):          # timeout de 500 ms
                continue
            raw  = receiver.recv()
            m    = pickle.loads(raw)

            if m.get("type") == "STOP":
                stop_event.set()
                break

            m["worker"] = wid
            result = convert(m)
            print(
                f"[{wid}]  {m['type']:<7}"
                f"  {m['original']:>8.2f} {result['unit_in']}"
                f"  →  {result['converted']:>9.4f} {result['unit_out']}"
            )
            sender.send(pickle.dumps(result))

        except Exception as exc:
            print(f"[{wid}] Erro: {exc}")

    receiver.close()
    sender.close()
    print(f"[{wid}] Encerrado.")


# ── Middleware principal ──────────────────────────────────────────────────────

def run_middleware(n_workers: int = 3):
    context    = zmq.Context()
    stop_event = threading.Event()

    # ── Sockets externos ─────────────────────────────────────────────────────
    # PULL: recebe do Producer (Máquina A)
    intake = context.socket(zmq.PULL)
    intake.connect(f"tcp://{IP_SRC}:{PORT1}")   # conecta ao Producer

    # PUSH: envia ao Consumer final (Máquina C)
    output = context.socket(zmq.PUSH)
    output.bind(f"tcp://*:{PORT2}")             # bind na própria máquina

    # ── Sockets internos (inproc) ────────────────────────────────────────────
    distributor = context.socket(zmq.PUSH)
    distributor.bind("inproc://tasks")

    collector = context.socket(zmq.PULL)
    collector.bind("inproc://results")

    print(f"[MIDDLEWARE] Escutando Producer em tcp://{IP_SRC}:{PORT1}")
    print(f"[MIDDLEWARE] Enviando ao Consumer em tcp://*:{PORT2}")
    print(f"[MIDDLEWARE] Workers de conversão: {n_workers}\n")

    # Inicia workers de conversão em threads
    threads = []
    for i in range(n_workers):
        t = threading.Thread(
            target=conversion_worker,
            args=(i + 1, context, stop_event),
            daemon=True,
        )
        t.start()
        threads.append(t)

    # ── Loop principal: distribui entrada e coleta resultados ────────────────
    poller = zmq.Poller()
    poller.register(intake,    zmq.POLLIN)
    poller.register(collector, zmq.POLLIN)

    try:
        while not stop_event.is_set():
            events = dict(poller.poll(500))

            # Dado novo do Producer → distribui para workers
            if intake in events:
                raw = intake.recv()
                m   = pickle.loads(raw)
                if m.get("type") == "STOP":
                    # Propaga STOP para todos os workers
                    for _ in range(n_workers):
                        distributor.send(pickle.dumps({"type": "STOP", "value": 0, "unit": ""}))
                    stop_event.set()
                else:
                    distributor.send(raw)

            # Resultado pronto de um worker → encaminha ao sink
            if collector in events:
                result_raw = collector.recv()
                output.send(result_raw)         # PUSH ao Consumer final

    except KeyboardInterrupt:
        print("\n[MIDDLEWARE] Interrompido.")
    finally:
        for t in threads:
            t.join(timeout=2)
        intake.close()
        output.close()
        distributor.close()
        collector.close()
        context.term()
        print("[MIDDLEWARE] Encerrado.")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    run_middleware(n)
