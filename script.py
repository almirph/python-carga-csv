import csv
import redis
import json

redis = redis.Redis(host='127.0.0.1', port=6379, password='', db=2)

def main():
    try:
        print('Iniciando execução...')
        with open('json/tabelas.json', 'r') as f:
            tabelas_distro = json.load(f)
            readPlanilhaCsv(tabelas_distro)
    except Exception as e:
        print(f'Erro ao executar o script:\n{e}')

def readPlanilhaCsv(tabelas_distro):
    for tabela_distro in tabelas_distro:
        doReadPlanilhaCsv(tabela_distro)

def doReadPlanilhaCsv(tabela_distro):
    print(f'Lendo planilha: {tabela_distro["nomePlanilha"]}')
    with open('planilhas/' + tabela_distro['nomePlanilha'], mode='r', encoding='ANSI') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=tabela_distro['delimiter'])
        line_count = 0
        for row in csv_reader:
            try:
                if line_count > 0:
                    id_tabela = buildIdTabela(tabela_distro, row)
                    for campo in tabela_distro['campos']:
                        redis.hset(id_tabela, campo, row[campo])
            except Exception as e:
                print(f'Erro linha {line_count}\n {e}')
            line_count += 1
        print(f'Processed {line_count} lines.')

def buildIdTabela(tabela_distro, row):
    id_tabela = ''
    for id_tabela_string in tabela_distro['ids']:
        id_tabela = id_tabela + row[id_tabela_string]
    return tabela_distro['nomeTabela'] + ':' + id_tabela

main()