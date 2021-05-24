import csv
import requests
import json
from multiprocessing import Pool
from data_csv import *

def write_csv(filename, data, fields):
    with open(filename, 'a') as file:
        writer = csv.DictWriter(file, fieldnames=fields, delimiter = ';', lineterminator="\r")
        writer.writerow(data)


def get_json(url):
    r = requests.get(url)
    return r.json()['items']


def post_json(url,payload=None):
    headers = {
        'Content-Type': 'application/json',
    }
    r = requests.post(url, headers=headers, data=payload)
    return r.json()['items']


def post_request(id, pageNumber):
    url = f'https://cataloguniversal.autodoc.ru/api/catalogs/universal/products/withprice/{id}?pageSize=12&pageNumber={pageNumber}&clientId=375'

    payload = {'filters': [], 'order': 1, 'mark': 0}
    payload = json.dumps(payload)

    res = post_json(url, payload)

    return res


def pars_oils(data, data_csv, fields):
    for category in data['children'][:2]:
        pageNumber = 1
        while True:
            res = post_request(category["id"], pageNumber)

            if (not res): break

            for el in res:
                url_ = f'https://webapi.autodoc.ru/api/manufacturer/{el["manufacturer"]["id"]}/sparepart/{el["partNumber"]}'

                r = requests.get(url_).json()
                for field in r['properties']:
                    if (field['name'] in data_csv): data_csv[field['name']] = field['value']
                data_csv['Цена'] = el['minimalPrice']
                data_csv['Производитель'] = el['manufacturer']['name']
                data_csv['Изображение'] = el.get('photoUrl', '')
                write_csv('result\oils.csv', data_csv, fields)

                if (data_csv['Изображение']):
                    img = requests.get(el['photoUrl'])
                    out = open(f"result\img_oils\\{el['photoUrl'].split('/')[-1]}.jpg", "wb")
                    out.write(img.content)
                    out.close()

            pageNumber += 1


def pars_autochemistry(data, filename, data_csv, fields, url_photo):
    for category in data['children']:
        for focus in category['children']:
            for type in focus['children']:
                parser(filename, data_csv, fields, type['id'], url_photo)


def parser(filename, data_csv, fields, id, url_photo):
    pageNumber = 1
    while True:
        res = post_request(id, pageNumber)
        if (not res): break

        for el in res:
            for field in el['properties']:
                if (field['name'] in data_csv): data_csv[field['name']] = field['value']
            data_csv['Цена'] = el['minimalPrice']
            data_csv['Изображение'] = el.get('photoUrl', '')
            write_csv(filename, data_csv, fields)

            if (data_csv['Изображение']):
                img = requests.get(el['photoUrl'])
                out = open(f"{url_photo}\\{el['photoUrl'].split('/')[-1]}.jpg", "wb")
                out.write(img.content)
                out.close()

        pageNumber += 1


def main():
    url = 'https://cataloguniversal.autodoc.ru/api/catalogs/universal/categories'

    tasks = []
    with Pool(5) as p:
        tasks.append(p.apply_async(
            parser,
            args=('result\\tires.csv' ,data_csv_tires, fields_tires, 1536, 'result\img_tires'),
            callback=lambda arg: print('Парсинг шин завершён')
        ))

        tasks.append(p.apply_async(
            parser,
            args=('result\\brushes.csv', data_csv_brushes, fields_brushes, 1581, 'result\img_brushes'),
            callback=lambda arg: print('Парсинг щёток завершён')
        ))

        tasks.append(p.apply_async(
            parser,
            args=('result/disks.csv', data_csv_disks, fields_disks, 530, 'result/img_disks'),
            callback=lambda arg: print('Парсинг дисков завершён')
        ))

        autochemistry = get_json(url)[2]
        tasks.append(p.apply_async(pars_autochemistry,
                      args=(autochemistry, 'result\\autochemistry.csv', data_csv_autochemistry, fields_autochemistry, 'result\img_autochemistry'),
                      callback=lambda arg: print('Парсинг автохимии завершён')))

        oils = get_json(url)[0]
        tasks.append(p.apply_async(pars_oils,
                      args=(oils, data_csv_oils, fields_oils),
                      callback=lambda arg: print('Парсинг масел завершён')))

        for task in tasks:
            task.wait()


if __name__ == '__main__':
    main()