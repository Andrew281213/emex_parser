import sys
from pprint import pprint

import requests
import xlrd
import xlwt

from fake_useragent import UserAgent
from loguru import logger
from multiprocessing import Pool
from tqdm import tqdm


INPUT_FILEPATH = "in_data.xls"  # Путь к файлу с прайс-листом
MAX_THREADS = 2  # Максимальное кол-во потоков
MAX_ATTEMPTS = 5  # Максимальное кол-во попыток получения ответа от сайта
IN_FILE_SKIP_ROWS = 3  # Кол-во строк для пропуска при чтении файла(заголовок)
OFFERS_CNT = 20  # Максимальное кол-во предложений

# Astroproxy
PROXY_ADDRESS = "109.248.7.162:10019"   # Адрес прокси
PROXY_LOGIN = "ab4078"                  # Логин прокси
PROXY_PASSWORD = "8e9cca"               # Пароль прокси

# Список альтернативных названий
ALTERNATIVES = {
	"Motorcraft": "Ford",
	"Mopar": "Chrysler"
}
# Координаты и id локации - Россия, Москва, Лучников переулок, 4с2
location = {
	"latitude": 55.7583,
	"location_id": 31980,
	"longitude": 37.6318
}

# Поиск локаций - центр москвы
LEFT_BOTTOM_LATITUDE = "55.656172326727706"  # Широта в левом нижнем углу
LEFT_BOTTOM_LONGITUDE = "36.981801259765604"  # Долгота в левом нижнем углу
RIGHT_TOP_LATITUDE = "55.85520068563045"  # Широта в правом верхнем углу
RIGHT_TOP_LONGITUDE = "38.25346874023434"  # Долгота в левом нижнем углу
LONGITUDE = "37.617634999999986"  # Долгота
LATITUDE = "55.75581399999372"  # Широта

ua = UserAgent()
headers = {
	"user-agent": ua.random
}
proxies = {
	"https": f"http://{PROXY_LOGIN}:{PROXY_PASSWORD}@{PROXY_ADDRESS}"
}


def _request(url, params=None):
	"""Отправляет get запрос по ссылке

	:param str url: Ссылка
	:param dict params: Параметры запроса
	:return: Текст страницы
	:rtype: dict or list[dict] or None
	"""
	if params["make"] == "johnsen's":
		url += "?make=Johnsen%E2%80%99s"
		del params["make"]
	try:
		logger.debug(f"Get {url}")
		r = requests.get(url, headers=headers, params=params, proxies=proxies, timeout=10)
		r.raise_for_status()
		logger.debug(f"Get {r.status_code} {r.url}")
		return r.json()
	except Exception as e:
		logger.debug(f"Error get {url} {e}")


def attempt_request(url, params=None):
	"""Отправляет запрос по ссылке до тех пор, пока не закончатся попытки(MAX_ATTEMPTS)

	:param str url: Ссылка
	:param dict params: Параметры запроса
	:return: Текст страницы или None
	:rtype: dict or list[dict] or None
	"""
	attempt = 1
	while attempt <= MAX_ATTEMPTS:
		logger.debug(f"Get {attempt}/{MAX_ATTEMPTS} {url}")
		attempt += 1
		content = _request(url, params)
		if content:
			return content
	else:
		logger.warning(f"Не удалось загрузить {url}")


def load_price_list():
	"""Загружает данные из .xls файла

	:return: Данные из файла
	:rtype: list[dict]
	"""
	logger.debug("Start load price list")
	wb = xlrd.open_workbook(INPUT_FILEPATH)
	sheet = wb.sheet_by_index(0)
	skip_rows = IN_FILE_SKIP_ROWS
	data = []
	for row in sheet.get_rows():
		if skip_rows > 0:
			skip_rows -= 1
			continue
		data.append({
			"producer": row[0].value,
			"article": row[1].value,
			"name": row[2].value,
			"price": row[3].value,
			"cnt": row[4].value if row[4].value else 0
		})
	logger.info(f"Успешно загружено {len(data)} строк")
	return data
	

def search_locations(item, item_producer=None):
	"""Поиск точек продажи

	:param dict item: Словарь с данными о товаре
	:param str or None item_producer: Название производителя, если None - используется производитель указанный в item
	:return: Массив списков с данными точки
	:rtype: list[dict]
	"""
	if item_producer is None:
		item_producer = item["producer"]
	item_producer = item_producer.lower().strip()
	url = f"https://emex.ru/api/search/search?detailNum={item['article'].replace('-', '').strip()}&make={item_producer}" \
	      f"&leftBottomLatitude={LEFT_BOTTOM_LATITUDE}&leftBottomLongitude={LEFT_BOTTOM_LONGITUDE}" \
	      f"&rightTopLatitude={RIGHT_TOP_LATITUDE}&rightTopLongitude={RIGHT_TOP_LONGITUDE}" \
	      f"&shouldDebouncing=true&longitude={LONGITUDE}&latitude={LATITUDE}"
	logger.debug(f"Search producer {item_producer} {url}")
	page_data = attempt_request(url)
	if page_data is None:
		logger.warning(f"Не удалось получить локации")
		return
	tmp = page_data.get("searchResult", {}).get("makes", {}).get("list", [])
	if len(tmp) == 0:
		return
	page_data = page_data.get("searchResult", {}).get("points", {}).get("list", [])
	locations_data = [{
		"location_id": page_item.get("locationId"),
		"longitude": page_item.get("longitude"),
		"latitude": page_item.get("latitude")
	} for page_item in page_data]
	if len(locations_data) > 0:
		logger.debug(f"Для {item['article']}, {item_producer} получено {len(locations_data)} локаций")
		return locations_data
	logger.warning(f"Не удалось получить предложения для {item['article']}, {item_producer}")
	return []


def parse_offers(item, alternative=False):
	"""Поиск предложений

	:param dict item: Словарь с данными о товаре
	:param bool alternative: Если True, то проверяется на повторы с предыдущими предложениями, иначе собирается все
	:return: Словарь с данными о товаре
	:rtype: dict
	"""
	item["offers"] = []
	article = item["article"].replace("-", "").strip()
	if alternative:
		make = ALTERNATIVES.get(item["producer"]).lower()
	else:
		make = item.get("producer").lower()
	make = make.strip()
	if make == "acura":
		make = "honda"
	elif make == "hyundai":
		make = "Hyundai / KIA"
	url = "https://emex.ru/api/search/search"
	params = {
		"make": make,
		"detailNum": article,
		"locationId": location["location_id"],
		"showAll": "true",
		"longitude": location["longitude"],
		"latitude": location["latitude"]
	}
	page_data = attempt_request(url, params)
	if make == "johnsen's":
		make = "johnsen’s"
	if page_data is None:
		logger.warning(f"Не удалось загрузить страницу предложений: {article}, {make}")
		return item
	originals = page_data.get("searchResult", {}).get("originals", [])
	for original in originals:
		if original.get("make").lower() == make.lower():
			break
	else:
		logger.warning(f"Не удалось получить предложения: {article}, {make}")
		return item
	offers_data = original.get("offers", [])
	logger.debug(f"Get offers data {article} {make} {location} offers cnt: {len(offers_data)}")
	for offer_item in offers_data[:OFFERS_CNT]:
		offer = {
			"delivery": offer_item.get("delivery", {}).get("value"),
			"remain": offer_item.get("quantity"),
			"price": offer_item.get("price", {}).get("value"),
			"rating": offer_item.get("rating2", {}).get("rating"),
			"id": offer_item.get("rating2", {}).get("code"),
			"alt": alternative
		}
		logger.debug(f"Get offer: {offer}")
		item["offers"].append(offer)
	return item


def _parse(item, item_producer=None):
	"""Сбор предложений о товарах

	:param dict item: Словарь с данными о товаре
	:param str item_producer: Название производителя, если None - используется производитель указанный в item
	:return: Словарь с данными о товаре
	:rtype: dict
	"""
	logger.debug(f"Start parse {item['article']}")
	if item_producer is not None:
		alt = True
	else:
		alt = False
	item = parse_offers(item, alternative=alt)
	if len(item.get("offers", [])) < OFFERS_CNT and item_producer is None and item.get(
			"producer") in ALTERNATIVES.keys():
		item["alt_producer"] = ALTERNATIVES.get(item.get("producer"))
		logger.debug(f"По {item.get('article')} {item.get('producer')} найдено {len(item.get('offers'))} предложений, "
		             f"ищу по брэнду {ALTERNATIVES.get(item['producer'])}")
		return _parse(item, item["alt_producer"])
	return item


def save_data(data):
	"""Сохранение данных о товарах

	:param list[dict] data: Список товаров
	"""
	
	def write_row(row, row_data):
		for c, cell_data in enumerate(row_data):
			sheet.write(row, c, cell_data)
	
	wb = xlwt.Workbook()
	sheet: xlwt.Worksheet = wb.add_sheet("Sheet 1")
	header = ["Брэнд", "Артикул", "Цена", "Наличие", "Срок доставки", "Рейтинг", "ID продавца"]
	write_row(0, header)
	row_number = 1
	for item in data:
		offers = item.get("offers")
		if len(offers) == 0:
			write_row(row_number, [item.get("producer"), item.get("article"), item.get("price"), 0, 0, 0, 0])
			row_number += 1
		for offer in offers[:OFFERS_CNT]:
			rating = offer.get("rating")
			if rating is not None:
				rating = rating.replace(",", ".")
			offer_producer = item.get("alt_producer") if offer["alt"] else item.get("producer")
			offer_row = [offer_producer, item.get("article"), offer.get("price"), offer.get("remain"),
			             offer.get("delivery"), rating, offer.get("id")]
			write_row(row_number, offer_row)
			row_number += 1
	wb.save("data.xls")


def parse():
	"""Запуск сбора предложений"""
	logger.info(f"Запуск парсера")
	price_list_data = load_price_list()
	data = []
	pbar = tqdm(total=len(price_list_data))
	with Pool(MAX_THREADS) as pool:
		for res in pool.imap(_parse, price_list_data):
			data.append(res)
			pbar.update()
	save_data(data)
	logger.info(f"Парсер завершил работу, результат сохранен в файл data.xls")


if __name__ == '__main__':
	logger.remove()
	logger.add(sys.stdout, level="INFO")
	parse()
