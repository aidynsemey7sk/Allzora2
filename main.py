import pandas as pd
import json
import time
from merge import get_data_from_xml_1, get_data_from_xml_2, get_data_from_json
import warnings
warnings.filterwarnings("ignore", 'This pattern has match groups')

start_time = time.time()

'''Создание трех DataFrame и сортировка'''
tr_df_1 = pd.DataFrame(get_data_from_xml_1('data_Soruce_1.xml'))  # 7062 str
tr_df_1 = tr_df_1.sort_values(by='ean_code')
tr_df_2 = pd.DataFrame(get_data_from_xml_2('data_Source_2.xml'))  # 11534
tr_df_2 = tr_df_2.sort_values(by='ean_code')
js_df = pd.DataFrame(get_data_from_json('data_Source_3.json'))  # 31368 str
js_df = js_df.sort_values(by='ean_code')

print(tr_df_1)
print(tr_df_2)
print(js_df)
# test = tr_df_1.measure.unique()
# print(test)

'''Объединение Dataframe по EAN коду'''
merged_inner = pd.merge(left=tr_df_1, right=tr_df_2, left_on='ean_code', right_on='ean_code')
merged_inner2 = pd.merge(left=merged_inner, right=js_df, left_on='ean_code', right_on='ean_code')

'''Получение списка ean_code которые мы уже выбрали
    для последующего удаления из начальных DF
'''
delete_list = merged_inner2['ean_code'].values.tolist()

"""Удаляем строки из начальных DF по списку для удаления"""
tr_df_1 = tr_df_1.loc[~tr_df_1['ean_code'].isin(delete_list)]  # Осталось 6318 строк
tr_df_2 = tr_df_2.loc[~tr_df_2['ean_code'].isin(delete_list)]  # Осталось 10790 строк
js_df = js_df.loc[~js_df['ean_code'].isin(delete_list)]  # Осталось 30624 строк

'''Соединяем первый и второй датафрейм'''
tr_df_1 = tr_df_1.loc[tr_df_1['tester'] != 'True']
merged_df1_and_df2 = pd.merge(left=tr_df_1, right=tr_df_2, on=[
    "MANUFACTURER", "SIZE", 'brandline', 'linea_name_lower',
    'category', 'manufacturer_lower', 'gender', 'measure',
])

'''Получим уникальные имена производителей'''
unique_name_for_df_1 = merged_df1_and_df2.MANUFACTURER.unique()


def get_all_MANUFACTURER_in_js_df(manufacturer_list: list) -> list:
    unique_name = [x.upper() for x in manufacturer_list]
    all_brand_list = []
    for name in unique_name:
        name = name.split()
        brand = js_df[js_df['MANUFACTURER'].str.contains(name[0])]
        brand = brand.drop_duplicates(subset=['MANUFACTURER'])
        all_brand_list += brand['MANUFACTURER'].values.tolist()

    return all_brand_list


all_MANUFACTURER_in_js_df = get_all_MANUFACTURER_in_js_df(unique_name_for_df_1)

'''Получим только те бренды, что есть во всех трех датафреймах'''
js_df = js_df.loc[js_df['MANUFACTURER'].isin(all_MANUFACTURER_in_js_df)]

# merged_union_and_js_df = pd.merge(left=js_df, right=merged_df1_and_df2, on=["MANUFACTURER"])
merged_file = merged_df1_and_df2.merge(js_df, on=['manufacturer_lower',
                                                  'linea_name_lower',
                                                  'SIZE', 'category', 'gender',
                                                  'measure',
                                                  ])

print(merged_file)
# print(merged_union_and_js_df)














#
# def merge_manufacturer(name_manufacturer: str):
#     try:
#         ts = tr_df_1.loc[tr_df_1['MANUFACTURER'] == name_manufacturer]
#         ts1 = tr_df_2.loc[tr_df_2['MANUFACTURER'] == name_manufacturer]
#         ''''''
#         js_name_manufacturer = name_manufacturer.upper()
#         js_name_manufacturer = js_name_manufacturer.split()
#         ts2 = js_df[js_df['MANUFACTURER'].str.contains(js_name_manufacturer[0])]
#         '''Создадим копию для поиска уникальных строк'''
#         all_manufacturer_name = ts2.copy()
#         '''Вырежем все дубликаты имён производителей'''
#         unique_manufacturer_name = all_manufacturer_name.drop_duplicates(subset=['MANUFACTURER'])
#         '''Получим уникальное количество производителей производителей'''
#         count_unique_name = unique_manufacturer_name.shape[0]
#         manufacture_name = unique_manufacturer_name.iloc[0]['MANUFACTURER']
#         '''Создадим и заполним его всеми вариантами имени производителся'''
#         ts2 = unique_manufacturer_name.iloc[0]['MANUFACTURER']
#         appended_data = []
#         if count_unique_name > 1:
#             for i in range(count_unique_name):
#                 new_df = js_df.loc[js_df['MANUFACTURER'] == unique_manufacturer_name.iloc[i]['MANUFACTURER']]
#                 appended_data.append(new_df)
#         '''Объеденим в итоговый DF'''
#         if appended_data:
#             appended_data = pd.concat(appended_data)
#             ts2 = appended_data
#     except:
#         # print('Такого бренда нет в одном из датафреймов')
#         pass
#     row1_ean_list = []
#     row_ean_list = []
#
#     two_ean_code = []
#
#     for i, row in ts.iterrows():
#         brand_list = row['brandline'].split()
#         if row['tester']:
#             brand_list.append('tester')
#         size = row['SIZE']
#
#         """Обработка DF 2"""
#         fuzzi_list = []
#         fuzzi_list_2 = []
#         for x, row1 in ts1.iterrows():
#             size1 = row1['SIZE']
#             fuzz_data = row['name']
#             fuzz_data_1 = row1['name'] + ' ' + row1['SIZE'] + ' ' + row1['measure'] + ' ' + row1['gender']
#             ean_code = row['ean_code']
#
#             ''' Получим список с двумя словарями, равными по ean_code,
#                 В список row_ean_list запишем еan_code которые уже попали в итоговый список, что бы знать какие
#                 строки уже не надо обрабатывать, чем кратно увеличим скорость обработки.
#             '''
#             if str(row['ean_code']) == str(row1['ean_code']):
#                 row_ean_list.append(row['ean_code'])
#                 m = [
#                     {
#                         'ean_code': row['ean_code'], 'name': row['name'],
#                         "source_name": row['source_name'], 'id': row['id'],
#                         'SIZE': row['SIZE']
#                     },
#                     {
#                         'ean_code': row1['ean_code'], 'name': row1['name'],
#                         "source_name": row1['source_name'], 'id': row1['id']
#                     }
#                 ]
#                 '''Сформируем данныe для сравнения в третьем DF'''
#                 two_ean_code.append(m)
#
#             '''Выберем все вариации с рейтингом больше 80 и проверим на вхождение в список row_ean_list
#             запишем в список fuzzi_list'''
#             if fuzz.token_sort_ratio(fuzz_data, fuzz_data_1) > 80:
#                 if size == size1 and row['ean_code'] not in row1_ean_list:
#                     rating = fuzz.token_sort_ratio(fuzz_data, fuzz_data_1)
#                     fuzzi_list.append([
#                         {
#                             'ean_code': ean_code, 'fuzz_data': fuzz_data,
#                             'id': row['id'], 'source_name': row['source_name']
#                         },
#
#                         {
#                             'ean_code': row1['ean_code'], 'fuzz_data_1': fuzz_data_1, 'id': row1['id'],
#                             'source_name': row1['source_name'], 'rating': rating
#                         }
#                     ])
#
#                     row1_ean_list.append(row1['ean_code'])
#
#         '''Из списка fuzzi_list выберем значения с наибольшим рейтингом'''
#         if fuzzi_list:
#             if len(fuzzi_list) > 1:
#                 sorted_salaries = sorted(fuzzi_list, key=lambda d: d[1]['rating'])
#                 res = sorted_salaries[-1]
#             else:
#                 res = fuzzi_list[0]
#
#             """Обработка DF 3"""
#             for x, row2 in ts2.iterrows():
#                 fuzz_data_2 = row2['MANUFACTURER'] + ' ' + row2['name']
#
#                 '''Сначала обработаем данные из two_ean_code,
#                  так как они уже совпали'''
#
#                 two_ean_fuzzy_list = []
#                 for z in two_ean_code:
#                     new_fuzz_data = z[0]['name']
#                     if fuzz.token_sort_ratio(new_fuzz_data, fuzz_data_2) > 71:
#                         if str(row2['SIZE']) == str(z[0]['SIZE']) and row['ean_code'] not in row_ean_list:
#                             new_rating = fuzz.token_sort_ratio(new_fuzz_data, fuzz_data_2)
#                             two_ean_fuzzy_list.append(
#                                 [{
#                                     'source_name': z[0]['source_name'],
#                                     'name': z[0]['name'],
#                                     'ean_code': z[0]['ean_code'],
#                                     'id': z[0]['id'],
#                                 },
#                                 {
#                                     'source_name': z[1]['source_name'],
#                                     'name': z[1]['name'],
#                                     'ean_code': z[1]['ean_code'],
#                                     'id': z[1]['id'],
#                                 },
#                                 {
#                                     'source_name': row2['source_name'],
#                                     'name': row2['name'],
#                                     'ean_code': row2['ean_code'],
#                                     'id': row2['id'],
#                                     'rating': new_rating
#
#                                 }
#                                 ]
#                             )
#                 '''Проверим и отправим результат'''
#                 if two_ean_fuzzy_list:
#
#                     if len(two_ean_fuzzy_list) == 1:
#                         res.append(two_ean_fuzzy_list[0][:-1])
#                     if len(two_ean_fuzzy_list) > 1:
#                         sorted_salaries = sorted(two_ean_fuzzy_list, key=lambda d: d[2]['rating'])
#                         print(sorted_salaries[-1])
#                         # print(sorted_salaries[-1][0][2])
#
#
#
#                 # print(row)
#                 '''Сравним с совпадающими данными по ean_code,
#                                 выберем все вариации с рейтингом больше 58'''
#                 # if matching_data:
#                 #     if fuzz.token_sort_ratio(matching_data, fuzz_data_2) > 58:
#                 #         if row['SIZE'] == row2['SIZE']:
#                 #             print(matching_data, row1['name'], fuzz_data_2)
#
#                 '''Выберем все вариации с рейтингом больше 68 и проверим на вхождение в список row_ean_list
#                 запишем в список fuzzi_list'''
#                 if fuzz.token_sort_ratio(res[0]['fuzz_data'], fuzz_data_2) > 58:
#                     if size in fuzz_data_2.split():
#                         rating = fuzz.token_sort_ratio(res[0]['fuzz_data'], fuzz_data_2)
#                         if 'tester' in res[0]['fuzz_data'].split():
#                             rating = rating - 1
#                         fuzzi_list_2.append([
#                             {'ean_code': ean_code, 'fuzz_data': fuzz_data, 'id': row1['id'],
#                              'source_name': row1['source_name']},
#                             {'ean_code': row2['ean_code'], 'fuzz_data_1': fuzz_data_2, 'id': row2['id'],
#                              'name': row2['name'],
#                              "source_name": row2['source_name'], 'rating': rating}
#                         ])
#
#             if fuzzi_list_2:
#                 if len(fuzzi_list_2) > 1:
#                     sorted_salaries = sorted(fuzzi_list_2, key=lambda d: d[1]['rating'])
#                     res1 = sorted_salaries[-1]
#                     if res1[0]['ean_code']:
#                         res.append(res1[-1])
#                 else:
#                     if res1[0]['ean_code']:
#                         res1 = fuzzi_list_2[0]
#                         res.append(res1[-1])
#                 # print(res)
#
#             if len(res) > 2:
#                 result_list.append(res)
#
#                 result = [
#                     {
#                         'source_name': row['source_name'],
#                         'name': row['name'],
#                         'ean_code': row['ean_code'],
#                         'id': row['id'],
#                     },
#                     {
#                         'source_name': row1['source_name'],
#                         'name': row1['name'],
#                         'ean_code': row1['ean_code'],
#                         'id': row1['id'],
#                     },
#                     {
#                         'source_name': row2['source_name'],
#                         'name': row2['name'],
#                         'ean_code': row2['ean_code'],
#                         'id': row2['id'],
#                     },
#                 ]
#                 # print('res', res)
#                 # print('RESULT', result)
#
#
# '''Запустим цикл для получения объединенных DF по имени производителя'''
# cnt = 0
# not_duplicate_list = []
# compare_products = []
# all_manufacturer_name = tr_df_1
# unique_manufacturer_name = all_manufacturer_name.drop_duplicates(subset=['MANUFACTURER'])
# for i, row4 in unique_manufacturer_name.iterrows():
#
#     brand = row4['MANUFACTURER']
#     try:
#         if brand not in not_duplicate_list:
#             # merge_manufacturer('Hugo Boss')
#             merge_manufacturer(brand)
#             cnt += 1
#             not_duplicate_list.append(brand)
#
#             # if cnt == 2:
#             #     break
#     except Exception as ex:
#         # print('Этот бренд мы уже обходили')
#         pass
# print(len(result_list))  # 77
#
# print(merged_inner2.shape)
# '''Распакуем объединенный список и прибавим к итоговому списку'''
# for i, row in merged_inner2.iterrows():
#     res = [
#         {
#             'source_name': row['source_name_x'],
#             'name': row['name_x'],
#             'ean_code': row['ean_code'],
#             'id': row['id_x'],
#         },
#         {
#             'source_name': row['source_name_y'],
#             'name': row['name_y'],
#             'ean_code': row['ean_code'],
#             'id': row['id_y'],
#         },
#         {
#             'source_name': row['source_name'],
#             'name': row['name'],
#             'ean_code': row['ean_code'],
#             'id': row['id'],
#         },
#     ]
#     # print(res)
#     result_list.append(res)
#
# print(len(result_list))  # 77
# # for i in result_list:
# #     print(i)
# print(len(result_list))
# with open("db.json", "w") as file:
#     json.dump({'compare_products': result_list}, file)
# print("--- %s seconds -func-" % (time.time() - start_time))
