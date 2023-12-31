import asyncio
import pymodbus.client as ModbusClient
from pymodbus.exceptions import ModbusIOException
import getmac
import time
import firebase_admin
from firebase_admin import credentials, db, auth

# Definição de variáveis
host = "192.168.1.10"
port = 502
mac_address = getmac.get_mac_address().replace(":", "")


# Autenticação Firebase
cred = credentials.Certificate("esp32.json")
firebase_admin.initialize_app(cred, {'databaseURL': 'https://esp32-001-b5578-default-rtdb.firebaseio.com/'})

# Fazer login com email e senha
try:
    user = auth.get_user_by_email("mestre@mestre.com")
    print("Autenticação bem-sucedida!")
except auth.AuthError as e:
    print(f"Erro de autenticação: {e}")

client = ModbusClient.ModbusTcpClient(host, port=port)

# Lista global para armazenar os registros pendentes
registradores_pendentes = []

async def write_registers(register, value):
    # Escrever no registrador especificado
    try:
        client.write_registers(register, [value], unit=1)
        print(f"Valor {value} escrito no registrador {register}")
    except Exception as e:
        print(f"Erro ao processar registrador {register}: {e}")

async def read_register(register):
    try:
        # Ler o valor do registrador especificado
        result = client.read_holding_registers(register, 1, unit=1)
        return result.registers[0]
    except ModbusIOException:
        print(f"Erro na leitura do registrador {register}")
        return None

async def process_pending_registers():
    # Processa registros pendentes
    while registradores_pendentes:
        register, value = registradores_pendentes.pop(0)  # Retire o primeiro registro da lista

        try:
            print(f"Processando registro pendente - Registrador: {register}, Valor: {value}")
            await write_registers(register, value)
        except Exception as e:
            print(f"Erro ao processar registrador {register}: {e}")

    # Aguarda um curto período antes de verificar novamente
    await asyncio.sleep(0.1)


def on_registradores_input_change(event):
    print("Change detected in RegistradoresInput")
    print("Event data:", event.data)
    print(registradores_pendentes)

    idRegistradoresInput = event.data
    if idRegistradoresInput:
        print("Estrutura original de idRegistradoresInput:", idRegistradoresInput)
        if isinstance(idRegistradoresInput, list):
            idRegistradoresInput = [reg for reg in idRegistradoresInput if reg is not None]
            idRegistradoresInput = {str(reg.get('idRegistrador', '')): reg for reg in idRegistradoresInput}
        elif isinstance(idRegistradoresInput, dict):
            pass
        else:
            print("Estrutura desconhecida de idRegistradoresInput:", idRegistradoresInput)
            return
        print("Estrutura após conversão:", idRegistradoresInput)
        for register_number, register_data in idRegistradoresInput.items():
            try:
                value = register_data.get('valor')
                if value is not None:
                    value = int(value)
                    registradores_pendentes.append((int(register_number), value))
                    print(f"Registro pendente adicionado: {register_number}, {value}")
            except Exception as e:
                print(f"Erro ao processar registrador {register_number}: {e}")
        
        # Remover os dados de RegistradoresInput/{mac_address} após processar os registros pendentes
        registradores_input_ref.delete()

    print(registradores_pendentes)


# Adicionar o observador para a pasta 'RegistradoresInput/{mac_address}'
registradores_input_ref = db.reference(f"RegistradoresInput/{mac_address}")



async def run_modbus_client():
    c = 1
    timestampAntigo = 0

    try:
        registradores_input_ref.listen(callback=on_registradores_input_change)
    except Exception as e:
        print(f"Erro durante Listen: {e}")

    while True:
    
        # Verifica se há registros pendentes para processar
        if registradores_pendentes:
            print(registradores_pendentes)
            await process_pending_registers()
        
        
        try:
            # Conectar ao servidor Modbus
            client.connect()

            timestamp = int(time.time() * 1000)
            if timestamp >= timestampAntigo + 10000:
                registradores_ref = db.reference(f"Registradores/{mac_address}")
                print("Conseguiu acessar")

                idRegistradores = registradores_ref.get()
                if idRegistradores:
                    print("Estrutura original de idRegistradores:", idRegistradores)

                    if isinstance(idRegistradores, list):
                        # Remover entradas None da lista original
                        idRegistradores = [reg for reg in idRegistradores if reg is not None]

                        # Se idRegistradores for uma lista, convertemos para um mapeamento
                        idRegistradores = {str(reg.get('idRegistrador', '')): reg for reg in idRegistradores}
                    elif isinstance(idRegistradores, dict):
                        # Se idRegistradores já for um mapeamento, usamos como está
                        pass
                    else:
                        print("Estrutura desconhecida de idRegistradores:", idRegistradores)
                        continue

                    # Remover elementos com isInput = True
                    idRegistradores = {
                        key: value
                        for key, value in idRegistradores.items()
                        if not value.get('isInput', False)
                    }

                    print("Estrutura após conversão:", idRegistradores)

                    for register_number in idRegistradores:
                        try:
                            value = await read_register(int(register_number))
                            print("value ", value)
                            if value is not None:
                                timestamp = int(time.time() * 1000)
                                data = {
                                    "data": value,
                                    "time": timestamp
                                }
                                print(data)
                                # Criar referência específica para o registrador atual
                                register_ref = db.reference(f"Registros/{mac_address}/{register_number}")
                                register_ref.update({timestamp: data})
                        except Exception as e:
                            print(f"Erro ao processar registrador {register_number}: {e}")
                    register_ref = db.reference(f"RegistradoresInput/{mac_address}")
                    data = {
                            "valor": c,
                            "idRegistrador": 8
                    }
                    register_ref.update({8: data})
                    c = c + 1
                    timestampAntigo = timestamp

        except Exception as e:
            print(f"Erro durante a execução: {e}")

        finally:
            # Aguardar um curto período antes de fechar a conexão
            await asyncio.sleep(0.1)

            # Fechar a conexão se estiver aberta
            if client.is_socket_open():
                client.close()

        await asyncio.sleep(0.1)

if __name__ == "__main__":
    # Iniciar as tarefas em paralelo
    loop = asyncio.get_event_loop()
    tasks = asyncio.gather(run_modbus_client())

    try:
        # Aguardar eventos indefinidamente
        loop.run_until_complete(tasks)

    except KeyboardInterrupt:
        # Capturar a interrupção do teclado (Ctrl+C) para encerrar o programa
        print("Programa encerrado pelo usuário.")

        # Cancelar as tarefas em execução
        for task in asyncio.all_tasks():
            task.cancel()

        # Aguardar o encerramento das tarefas
        loop.run_until_complete(asyncio.gather(*asyncio.all_tasks()))

    finally:
        # Fechar o loop
        loop.close()
