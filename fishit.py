from bs4 import BeautifulSoup
import requests

# import webbrowser
page = requests.get("https://alsuper.com/")
# page = requests.get("https://www.liverpool.com.mx/")

# page = requests.get(
#     "https://alsuper.com/categorias?agrupacion=1&departamento=1&familia=10&page=1&limit=100000")
kval_pairs = {"agrupacion": "1", "departamento": "1",
              "familia": "1", "page": "1", "limit": "60"}
page = requests.get("https://alsuper.com/categorias", params=kval_pairs)
print("Ingresando a: ", page.url)  # print the url that was fetched
print("Status: ", page.status_code, page.history)
# print("Content-Type: ", page.headers["content-type"])
# print("Content-Type: ", page.headers["content-type"][:9])
print(type(page.text))
print(page.headers)
soup = BeautifulSoup(page.text, "html.parser")
print(type(soup))

tag = soup("a")
tag2 = soup.find_all("a")
print(tag == tag2)

# list = soup.find_all("ul", class_="row list-unstyled products--list")
# print(list)

with open("alsuper.txt", "w") as f:
    f.write(page.text)
