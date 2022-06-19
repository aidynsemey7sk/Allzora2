import xml.etree.ElementTree as ET
import json
import time
import pandas as pd
import re

start_time = time.time()


def get_data_from_xml_1(xml_doc):
    count = 0
    tree = ET.parse(xml_doc)
    et_root = tree.getroot()
    all_products = et_root.findall('SHOPITEM')
    for product in all_products:
        products = product
        count += 1
        try:
            if products.find('EAN').text:
                ean = products.find('EAN').text
            else:
                ean = ''
        except Exception as ex:
            ean = ''

        id = products.find('id').text

        if products.find('MANUFACTURER').text:
            manufacturer = products.find('MANUFACTURER').text
            manufacturer_lower = manufacturer.lower()
        else:
            manufacturer = ''
            manufacturer_lower = ''
        if products.find('SIZE').text and products.find('SIZE').text != 'ml':
            size = products.find('SIZE').text
        else:
            size = ''

        if products.find('NAME').text:
            name = products.find('NAME').text
        else:
            name = ''

        if products.find('RANGE').text:
            brand_line = products.find('RANGE').text
            brand_line_lower = brand_line.lower()
            linea_name_lower = brand_line_lower
        else:
            brand_line = ''
            brand_line_lower = ''
            linea_name_lower = ''

        if products.find('CATEGORY_ROOT').text:
            category = products.find('CATEGORY_ROOT').text
        else:
            category = ''

        try:
            if products.find('GENDER').text:
                gender = products.find('GENDER').text
            else:
                gender = ''
        except Exception:
            gender = ''

        try:
            if products.find('DESCRIPTION').text:
                sort = products.find('DESCRIPTION').text
            else:
                sort = ''
        except Exception:
            sort = ''

        if re.findall('Parfém\b', sort):
            sort = 'Perfume'

        find_sort = name.split()

        if category == 'Parfémy':
            category = 'PERFUME'
        if category == 'Kosmetika':
            category = 'COSMETIC'

        if gender == 'Pánské':
            gender = 'M'
        elif gender == 'Dámské':
            gender = 'W'
        else:
            gender = 'U'

        new_name = name
        if "L'Oréal" in find_sort:
            manufacturer = manufacturer.replace("L'Oréal", "L'Oreal")
            brand_line = brand_line.replace("L'Oréal", "L'Oreal")
            new_name = new_name.replace("L'Oréal", "L'Oreal")

        if 'EDT' in find_sort:
            new_name = new_name.replace('EDT', 'Eau de Toilette')

        if 'EDP' in find_sort:
            new_name = new_name.replace('EDP', 'Eau de Parfum')

        if 'EDC' in find_sort:
            new_name = new_name.replace('EDC', 'Eau de Parfum')

        if 'vzorek' in find_sort:
            new_name = new_name.replace('vzorek', 'tester')
            new_name = new_name.replace(' (odstřik)', 'spray')

        if 'tester' in find_sort:
            tester = 'True'

        else:
            tester = 'False'

        volume = size.split(".")
        size = volume[0]
        measure = volume[1]

        if measure[-2:] == 'ml':
            measure = 'ml'
        if measure[-1:] == 'g':
            measure = 'g'
        if measure[-1:] == 'l':
            measure = 'ml'
        if measure[-1:] == 'ks':
            measure = 'ps'



        # print(measure.replace('0000 ml', 'ml'))

        dct = {'id': id, 'ean_code': ean, 'MANUFACTURER': manufacturer,
               'brandline': brand_line, 'sort': sort, 'name': name,
               'SIZE': size, 'source_name': 'data_Source_1', 'tester': tester,
               'new_name': new_name, 'category': category, 'gender': gender,
               'manufacturer_lower': manufacturer_lower, 'measure': measure,
               'brand_line_lower': brand_line_lower,
               'linea_name_lower': linea_name_lower,
               }
        yield dct


def get_data_from_xml_2(xml_doc):
    count = 0
    tree = ET.parse(xml_doc)
    et_root = tree.getroot()
    all_products = et_root.findall('Product')
    for product in all_products:
        try:
            products = product
            ean = products.find('EAN').text
            manufacturer = products.find('Brand').text
            manufacturer_lower = manufacturer.lower()
            id = products.find('id').text
            weight = products.find('Weight').text
            brand_line = products.find('BrandLine').text
            brand_line_lower = brand_line.lower()
            linea_name_lower = brand_line_lower
            description = products.find('Description').text
            category = product.find('StockType').text
            sort = product.find('Sort').text
            measure = product.find('Weight_UnitOfMeasurement').text
            gender = product.find('Gender').text

            if gender == 'H':
                gender = 'M'
            elif gender == 'D':
                gender = 'W'
            else:
                gender = 'U'

            if products.find('ProductTranslation'):
                name = products.find('ProductTranslation').find('name').text
            else:
                name = ''

            weight = weight.split(",")[0]

            description = description + ' ' + weight + ' ' + measure
            new_name = description

            if 'Edt' in description:
                new_name = new_name.replace('Edt', 'Eau de Toilette')

            if 'Edp' in description:
                new_name = new_name.replace('Edp', 'Eau de Parfum')

            if 'Edc' in description:
                new_name = new_name.replace('Edc', 'Eau de Parfum')

            dct = {'id': id, 'ean_code': ean,
                   'name': name, 'MANUFACTURER': manufacturer,
                   'brandline': brand_line, 'measure': measure,
                   'description': description, 'category': category,
                   'sort': sort, 'gender': gender, 'SIZE': weight,
                   'source_name': 'data_Source_2', 'new_name': new_name,
                   'manufacturer_lower': manufacturer_lower,
                   'brand_line_lower': brand_line_lower,
                   'linea_name_lower': linea_name_lower,
                   }
            yield dct
            count += 1
        except Exception as er:
            print(er)


def get_data_from_json(json_doc):
    with open(json_doc) as f:
        all_products = json.load(f, strict=False)

    # count = 0
    for product in all_products:
        ean_code = str(product['EANs'][0])
        id = str(product['Id'])
        name = str(product['name'])
        size = product['Contenido']
        size = size.split()
        measure = ' '
        category = product['Families'][0]

        if category == 'Perfumes':
            category = 'PERFUME'
        else:
            category = 'COSMETIC'

        if size:
            try:
                measure = size[1]
                size = int(size[0])
            except Exception as ex:
                size = size[0]
        if size == 'ml':
            size = 0

        if size:
            size = str(size)
        else:
            size = ''

        manufacturer = product['BrandName']
        brand_line = product['LineaName']
        linea_name = product['LineaName']
        brand_id = product['BrandId']
        manufacturer_lower = manufacturer.lower()
        linea_name_lower = linea_name.lower()
        brand_line_lower = brand_line.lower()
        gender = product['Gender']

        if gender == 'Man':
            gender = 'M'
        elif gender == 'Woman':
            gender = 'W'
        else:
            gender = 'U'

        dct = {'id': id, 'ean_code': ean_code, 'MANUFACTURER': manufacturer, 'brandline': brand_line,
               'LineaName': linea_name, 'name': name, 'SIZE': size, 'brand_id': brand_id,
               'measure': measure, 'category': category, 'manufacturer_lower': manufacturer_lower,
               'source_name': 'data_Source_3', 'linea_name': linea_name, 'linea_name_lower': linea_name_lower,
               'brand_line_lower': brand_line_lower, 'gender': gender,
               }
        yield dct
