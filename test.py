import re


text = 'Dámská toaletní voda - odstřik'

if re.findall('\bT|toaletní\svo', text):
    print('yes')
else:
    print('no')
print(re.search('([Parfé.])', text))

txt = "The rain in Spain"
x = re.search("^The.*Spain$", txt)
print(x)