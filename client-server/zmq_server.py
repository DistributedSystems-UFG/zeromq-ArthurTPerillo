import zmq
import json
import sys

#  Conversões suportadas 

def fahrenheit_to_celsius(f: float) -> float:
    """Converte Fahrenheit → Celsius."""
    return (f - 32) * 5 / 9

def pounds_to_kg(lbs: float) -> float:
    """Converte Libras → Quilogramas."""
    return lbs * 0.453592

def miles_to_km(mi: float) -> float:
    """Converte Milhas → Quilômetros."""
    return mi * 1.60934

def inches_to_cm(inch: float) -> float:
    """Converte Polegadas → Centímetros."""
    return inch * 2.54

CONVERSIONS = {
    "temp":   (fahrenheit_to_celsius, "°F",  "°C"),
    "weight": (pounds_to_kg,          "lbs", "kg"),
    "dist":   (miles_to_km,           "mi",  "km"),
    "length": (inches_to_cm,          "in",  "cm"),
}

#  Lógica do servidor 

def handle_request(raw: bytes) -> bytes:
    """
    Recebe uma requisição JSON, aplica a conversão e retorna
    uma resposta JSON.

    Protocolo de entrada:
      { "type": "temp"|"weight"|"dist"|"length", "value": <float> }

    Protocolo de saída (sucesso):
      { "status": "ok", "input": 98.6, "unit_in": "°F",
        "output": 37.0, "unit_out": "°C" }

    Protocolo de saída (erro):
      { "status": "error", "message": "<descrição>" }

    Mensagem especial:
      b"STOP"  →  encerra o servidor
    """
    text = raw.decode().strip()

    if text == "STOP":
        return b"STOP"                          # sinal interno de encerramento

    try:
        req = json.loads(text)
        conv_type = req.get("type", "").lower()
        value     = float(req["value"])

        if conv_type not in CONVERSIONS:
            raise ValueError(
                f"Tipo '{conv_type}' desconhecido. "
                f"Use: {list(CONVERSIONS.keys())}"
            )

        fn, unit_in, unit_out = CONVERSIONS[conv_type]
        result = fn(value)

        resp = {
            "status":   "ok",
            "type":     conv_type,
            "input":    value,
            "unit_in":  unit_in,
            "output":   round(result, 4),
            "unit_out": unit_out,
        }
    except (KeyError, ValueError, json.JSONDecodeError) as exc:
        resp = {"status": "error", "message": str(exc)}

    return json.dumps(resp).encode()


def run_server(port: int = 12345):
    context = zmq.Context()
    socket  = context.socket(zmq.REP)       # socket de resposta (REQ-REP)
    socket.bind(f"tcp://*:{port}")          # escuta em todas as interfaces

    print(f"[SERVER] Aguardando conexões na porta {port}…")
    print("[SERVER] Conversões disponíveis: temp | weight | dist | length")
    print("[SERVER] Envie STOP para encerrar.\n")

    while True:
        raw  = socket.recv()                # bloqueia até chegar mensagem
        resp = handle_request(raw)

        if resp == b"STOP":
            socket.send(b'{"status":"ok","message":"servidor encerrado"}')
            print("[SERVER] Mensagem STOP recebida. Encerrando.")
            break

        socket.send(resp)                   # envia resposta ao cliente

        # log local
        try:
            data = json.loads(resp)
            if data["status"] == "ok":
                print(
                    f"[SERVER] {data['input']} {data['unit_in']}"
                    f" → {data['output']} {data['unit_out']}"
                )
            else:
                print(f"[SERVER] Erro: {data['message']}")
        except Exception:
            pass

    socket.close()
    context.term()


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 12345
    run_server(port)
