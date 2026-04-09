import asyncio
import aiohttp
import aiofiles
import re
import os
import time
import json
import subprocess
import tempfile
import requests
import threading
import hashlib
import socket
import random
import urllib.parse
import ssl
import sys
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========= ДИАГНОСТИКА =========
print(f"🚀 Запуск парсера...")
print(f"📂 Текущая директория: {os.getcwd()}")
print(f"🐍 Python версия: {sys.version}")

# ========= ФАЙЛЫ =========
SOURCES_FILE = "sources.txt"
OUTPUT_FILE = "url.txt"
CLEAN_FILE = "url_clean.txt"
FILTERED_FILE = "url_filtered.txt"
NAMED_FILE = "url_named.txt"
ENCODED_FILE = "url_encoded.txt"
WORK_FILE = "url_work.txt"
LOG_FILE = "log.txt"
PROCESSED_FILE = "processed.json"
CACHE_FILE = "cache_results.json"
DEBUG_FILE = "debug_failed.txt"
XRAY_LOG_FILE = "xray_errors.log"

# ========= НАСТРОЙКИ =========
THREADS_DOWNLOAD = 50
CYCLE_DELAY = 3600
LOG_CLEAN_INTERVAL = 86400
CYCLES_BEFORE_DEBUG_CLEAN = 5

XRAY_MAX_WORKERS = 30
XRAY_TEST_URL = "https://www.gstatic.com/generate_204"
XRAY_TIMEOUT = 3
MAX_RETRIES = 2
RETRY_DELAY = 1

print(f"⚡ Настройки: XRAY_MAX_WORKERS={XRAY_MAX_WORKERS}, TIMEOUT={XRAY_TIMEOUT}")

cycle_counter = 0

# ========= РЕГУЛЯРКИ =========
VLESS_REGEX = re.compile(r"vless://[0-9a-fA-F\-]{36}@[^\s\"'<]+", re.IGNORECASE)
UUID_REGEX = re.compile(
    r"[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}"
)

# ========= СЛОВАРЬ ДОМЕНОВ =========
DOMAIN_NAMES = {
    'x5.ru': 'Пятёрочка', '5ka.ru': 'Пятёрочка', '5ka-cdn.ru': 'Пятёрочка',
    '5ka.static.ru': 'Пятёрочка', 'ads.x5.ru': 'Пятёрочка', 'perekrestok.ru': 'Перекрёсток',
    'vprok.ru': 'Перекрёсток', 'dixy.ru': 'Дикси', 'fasssst.ru': 'Fasssst',
    'rontgen.fasssst.ru': 'Fasssst', 'res.fasssst.ru': 'Fasssst', 'yt.fasssst.ru': 'Fasssst',
    'fast.strelkavpn.ru': 'StrelkaVPN', 'strelkavpn.ru': 'StrelkaVPN', 'maviks.ru': 'Maviks',
    'ru.maviks.ru': 'Maviks', 'a.ru.maviks.ru': 'Maviks', 'tree-top.cc': 'TreeTop',
    'a.ru.tree-top.cc': 'TreeTop', 'connect-iskra.ru': 'Iskra', '212-wl.connect-iskra.ru': 'Iskra',
    'vk.com': 'VK', 'vk.ru': 'VK', 'vkontakte.ru': 'VK', 'userapi.com': 'VK',
    'cdn.vk.com': 'VK', 'cdn.vk.ru': 'VK', 'id.vk.com': 'VK', 'id.vk.ru': 'VK',
    'login.vk.com': 'VK', 'login.vk.ru': 'VK', 'api.vk.com': 'VK', 'api.vk.ru': 'VK',
    'im.vk.com': 'VK', 'm.vk.com': 'VK', 'm.vk.ru': 'VK', 'sun6-22.userapi.com': 'VK',
    'sun6-21.userapi.com': 'VK', 'sun6-20.userapi.com': 'VK', 'sun9-38.userapi.com': 'VK',
    'sun9-101.userapi.com': 'VK', 'pptest.userapi.com': 'VK', 'vk-portal.net': 'VK',
    'stats.vk-portal.net': 'VK', 'akashi.vk-portal.net': 'VK', 'vkvideo.ru': 'VK Видео',
    'm.vkvideo.ru': 'VK Видео', 'queuev4.vk.com': 'VK', 'eh.vk.com': 'VK', 'cloud.vk.com': 'VK',
    'cloud.vk.ru': 'VK', 'admin.cs7777.vk.ru': 'VK', 'admin.tau.vk.ru': 'VK',
    'analytics.vk.ru': 'VK', 'api.cs7777.vk.ru': 'VK', 'api.tau.vk.ru': 'VK',
    'away.cs7777.vk.ru': 'VK', 'away.tau.vk.ru': 'VK', 'business.vk.ru': 'VK',
    'connect.cs7777.vk.ru': 'VK', 'cs7777.vk.ru': 'VK', 'dev.cs7777.vk.ru': 'VK',
    'dev.tau.vk.ru': 'VK', 'expert.vk.ru': 'VK', 'id.cs7777.vk.ru': 'VK', 'id.tau.vk.ru': 'VK',
    'login.cs7777.vk.ru': 'VK', 'login.tau.vk.ru': 'VK', 'm.cs7777.vk.ru': 'VK',
    'm.tau.vk.ru': 'VK', 'm.vkvideo.cs7777.vk.ru': 'VK Видео', 'me.cs7777.vk.ru': 'VK',
    'ms.cs7777.vk.ru': 'VK', 'music.vk.ru': 'VK Музыка', 'oauth.cs7777.vk.ru': 'VK',
    'oauth.tau.vk.ru': 'VK', 'oauth2.cs7777.vk.ru': 'VK', 'ord.vk.ru': 'VK', 'push.vk.ru': 'VK',
    'r.vk.ru': 'VK', 'target.vk.ru': 'VK', 'tech.vk.ru': 'VK', 'ui.cs7777.vk.ru': 'VK',
    'ui.tau.vk.ru': 'VK', 'vkvideo.cs7777.vk.ru': 'VK Видео', 'speedload.ru': 'Speedload',
    'api.speedload.ru': 'Speedload', 'chat.speedload.ru': 'Speedload', 'serverstats.ru': 'ServerStats',
    'cdnfive.serverstats.ru': 'ServerStats', 'cdncloudtwo.serverstats.ru': 'ServerStats',
    'furypay.ru': 'FuryPay', 'api.furypay.ru': 'FuryPay', 'jojack.ru': 'JoJack',
    'spb.jojack.ru': 'JoJack', 'at.jojack.ru': 'JoJack', 'tcp-reset-club.net': 'TCP Reset',
    'est01-ss01.tcp-reset-club.net': 'TCP Reset', 'nl01-ss01.tcp-reset-club.net': 'TCP Reset',
    'ru01-blh01.tcp-reset-club.net': 'TCP Reset', 'gov.ru': 'Госуслуги', 'kremlin.ru': 'Кремль',
    'government.ru': 'Правительство', 'duma.gov.ru': 'Госдума', 'genproc.gov.ru': 'Генпрокуратура',
    'epp.genproc.gov.ru': 'Генпрокуратура', 'cikrf.ru': 'ЦИК', 'izbirkom.ru': 'Избирком',
    'gosuslugi.ru': 'Госуслуги', 'sfd.gosuslugi.ru': 'Госуслуги', 'esia.gosuslugi.ru': 'Госуслуги',
    'bot.gosuslugi.ru': 'Госуслуги', 'contract.gosuslugi.ru': 'Госуслуги',
    'novorossiya.gosuslugi.ru': 'Госуслуги', 'pos.gosuslugi.ru': 'Госуслуги',
    'lk.gosuslugi.ru': 'Госуслуги', 'map.gosuslugi.ru': 'Госуслуги',
    'partners.gosuslugi.ru': 'Госуслуги', 'gosweb.gosuslugi.ru': 'Госуслуги',
    'voter.gosuslugi.ru': 'Госуслуги', 'gu-st.ru': 'Госуслуги', 'nalog.ru': 'ФНС',
    'pfr.gov.ru': 'ПФР', 'digital.gov.ru': 'Минцифры', 'adm.digital.gov.ru': 'Минцифры',
    'xn--80ajghhoc2aj1c8b.xn--p1ai': 'Минцифры', 'a.res-nsdi.ru': 'NSDI',
    'b.res-nsdi.ru': 'NSDI', 'a.auth-nsdi.ru': 'NSDI', 'b.auth-nsdi.ru': 'NSDI',
    'ok.ru': 'Одноклассники', 'odnoklassniki.ru': 'Одноклассники', 'cdn.ok.ru': 'Одноклассники',
    'st.okcdn.ru': 'Одноклассники', 'st.ok.ru': 'Одноклассники', 'apiok.ru': 'Одноклассники',
    'jira.apiok.ru': 'Одноклассники', 'api.ok.ru': 'Одноклассники', 'm.ok.ru': 'Одноклассники',
    'live.ok.ru': 'Одноклассники', 'multitest.ok.ru': 'Одноклассники', 'dating.ok.ru': 'Одноклассники',
    'tamtam.ok.ru': 'Одноклассники', '742231.ms.ok.ru': 'Одноклассники', 'ozon.ru': 'Ozon',
    'www.ozon.ru': 'Ozon', 'seller.ozon.ru': 'Ozon', 'bank.ozon.ru': 'Ozon', 'pay.ozon.ru': 'Ozon',
    'securepay.ozon.ru': 'Ozon', 'adv.ozon.ru': 'Ozon', 'invest.ozon.ru': 'Ozon',
    'ord.ozon.ru': 'Ozon', 'autodiscover.ord.ozon.ru': 'Ozon', 'st.ozone.ru': 'Ozon',
    'ir.ozone.ru': 'Ozon', 'vt-1.ozone.ru': 'Ozon', 'ir-2.ozone.ru': 'Ozon', 'xapi.ozon.ru': 'Ozon',
    'owa.ozon.ru': 'Ozon', 'learning.ozon.ru': 'Ozon', 'mapi.learning.ozon.ru': 'Ozon',
    'ws.seller.ozon.ru': 'Ozon', 'wildberries.ru': 'Wildberries', 'wb.ru': 'Wildberries',
    'static.wb.ru': 'Wildberries', 'seller.wildberries.ru': 'Wildberries',
    'banners.wildberries.ru': 'Wildberries', 'fw.wb.ru': 'Wildberries', 'finance.wb.ru': 'Wildberries',
    'jitsi.wb.ru': 'Wildberries', 'dnd.wb.ru': 'Wildberries', 'user-geo-data.wildberries.ru': 'Wildberries',
    'banners-website.wildberries.ru': 'Wildberries', 'chat-prod.wildberries.ru': 'Wildberries',
    'a.wb.ru': 'Wildberries', 'avito.ru': 'Avito', 'm.avito.ru': 'Avito', 'api.avito.ru': 'Avito',
    'avito.st': 'Avito', 'img.avito.st': 'Avito', 'sntr.avito.ru': 'Avito', 'stats.avito.ru': 'Avito',
    'cs.avito.ru': 'Avito', 'www.avito.st': 'Avito', 'st.avito.ru': 'Avito', 'www.avito.ru': 'Avito',
    **{f'{i:02d}.img.avito.st': 'Avito' for i in range(100)},
    'sberbank.ru': 'Сбербанк', 'online.sberbank.ru': 'Сбербанк', 'sber.ru': 'Сбербанк',
    'id.sber.ru': 'Сбербанк', 'bfds.sberbank.ru': 'Сбербанк', 'cms-res-web.online.sberbank.ru': 'Сбербанк',
    'esa-res.online.sberbank.ru': 'Сбербанк', 'pl-res.online.sberbank.ru': 'Сбербанк',
    'www.sberbank.ru': 'Сбербанк', 'vtb.ru': 'ВТБ', 'www.vtb.ru': 'ВТБ', 'online.vtb.ru': 'ВТБ',
    'chat3.vtb.ru': 'ВТБ', 's.vtb.ru': 'ВТБ', 'sso-app4.vtb.ru': 'ВТБ', 'sso-app5.vtb.ru': 'ВТБ',
    'gazprombank.ru': 'Газпромбанк', 'alfabank.ru': 'Альфа-Банк', 'metrics.alfabank.ru': 'Альфа-Банк',
    'tinkoff.ru': 'Тинькофф', 'tbank.ru': 'Тинькофф', 'cdn.tbank.ru': 'Тинькофф', 'hrc.tbank.ru': 'Тинькофф',
    'cobrowsing.tbank.ru': 'Тинькофф', 'le.tbank.ru': 'Тинькофф', 'id.tbank.ru': 'Тинькофф',
    'imgproxy.cdn-tinkoff.ru': 'Тинькофф', 'banki.ru': 'Банки.ру', 'yandex.ru': 'Яндекс',
    'ya.ru': 'Яндекс', 'dzen.ru': 'Дзен', 'kinopoisk.ru': 'Кинопоиск', 'yastatic.net': 'Яндекс',
    'yandex.net': 'Яндекс', 'mail.yandex.ru': 'Яндекс Почта', 'disk.yandex.ru': 'Яндекс Диск',
    'maps.yandex.ru': 'Яндекс Карты', 'api-maps.yandex.ru': 'Яндекс Карты',
    'enterprise.api-maps.yandex.ru': 'Яндекс Карты', 'music.yandex.ru': 'Яндекс Музыка',
    'yandex.by': 'Яндекс', 'yandex.com': 'Яндекс', 'travel.yandex.ru': 'Яндекс Путешествия',
    'informer.yandex.ru': 'Яндекс', 'mediafeeds.yandex.ru': 'Яндекс', 'mediafeeds.yandex.com': 'Яндекс',
    'uslugi.yandex.ru': 'Яндекс Услуги', 'kiks.yandex.ru': 'Яндекс', 'kiks.yandex.com': 'Яндекс',
    'frontend.vh.yandex.ru': 'Яндекс', 'favicon.yandex.ru': 'Яндекс', 'favicon.yandex.com': 'Яндекс',
    'favicon.yandex.net': 'Яндекс', 'browser.yandex.ru': 'Яндекс Браузер',
    'browser.yandex.com': 'Яндекс Браузер', 'api.browser.yandex.ru': 'Яндекс Браузер',
    'api.browser.yandex.com': 'Яндекс Браузер', 'wap.yandex.ru': 'Яндекс', 'wap.yandex.com': 'Яндекс',
    '300.ya.ru': 'Яндекс', 'brontp-pre.yandex.ru': 'Яндекс', 'suggest.dzen.ru': 'Дзен',
    'suggest.sso.dzen.ru': 'Дзен', 'sso.dzen.ru': 'Дзен', 'mail.yandex.com': 'Яндекс Почта',
    'yabs.yandex.ru': 'Яндекс', 'neuro.translate.yandex.ru': 'Яндекс Перевод', 'cdn.yandex.ru': 'Яндекс',
    'zen.yandex.ru': 'Дзен', 'zen.yandex.com': 'Дзен', 'zen.yandex.net': 'Дзен',
    'collections.yandex.ru': 'Яндекс Коллекции', 'collections.yandex.com': 'Яндекс Коллекции',
    'an.yandex.ru': 'Яндекс', 'sba.yandex.ru': 'Яндекс', 'sba.yandex.com': 'Яндекс',
    'sba.yandex.net': 'Яндекс', 'surveys.yandex.ru': 'Яндекс Опросы',
    'yabro-wbplugin.edadeal.yandex.ru': 'Яндекс', 'api.events.plus.yandex.net': 'Яндекс Плюс',
    'speller.yandex.net': 'Яндекс Спеллер', 'avatars.mds.yandex.net': 'Яндекс',
    'avatars.mds.yandex.com': 'Яндекс', 'mc.yandex.ru': 'Яндекс', 'mc.yandex.com': 'Яндекс',
    '3475482542.mc.yandex.ru': 'Яндекс', 'zen-yabro-morda.mediascope.mc.yandex.ru': 'Яндекс',
    'travel.yastatic.net': 'Яндекс', 'api.uxfeedback.yandex.net': 'Яндекс',
    'api.s3.yandex.net': 'Яндекс', 'cdn.s3.yandex.net': 'Яндекс',
    'uxfeedback-cdn.s3.yandex.net': 'Яндекс', 'uxfeedback.yandex.ru': 'Яндекс',
    'cloudcdn-m9-15.cdn.yandex.net': 'Яндекс', 'cloudcdn-m9-14.cdn.yandex.net': 'Яндекс',
    'cloudcdn-m9-13.cdn.yandex.net': 'Яндекс', 'cloudcdn-m9-12.cdn.yandex.net': 'Яндекс',
    'cloudcdn-m9-10.cdn.yandex.net': 'Яндекс', 'cloudcdn-m9-9.cdn.yandex.net': 'Яндекс',
    'cloudcdn-m9-7.cdn.yandex.net': 'Яндекс', 'cloudcdn-m9-6.cdn.yandex.net': 'Яндекс',
    'cloudcdn-m9-5.cdn.yandex.net': 'Яндекс', 'cloudcdn-m9-4.cdn.yandex.net': 'Яндекс',
    'cloudcdn-m9-3.cdn.yandex.net': 'Яндекс', 'cloudcdn-m9-2.cdn.yandex.net': 'Яндекс',
    'cloudcdn-ams19.cdn.yandex.net': 'Яндекс', 'http-check-headers.yandex.ru': 'Яндекс',
    'cloud.cdn.yandex.net': 'Яндекс', 'cloud.cdn.yandex.com': 'Яндекс',
    'cloud.cdn.yandex.ru': 'Яндекс', 'dr2.yandex.net': 'Яндекс', 'dr.yandex.net': 'Яндекс',
    's3.yandex.net': 'Яндекс', 'static-mon.yandex.net': 'Яндекс', 'sync.browser.yandex.net': 'Яндекс',
    'storage.ape.yandex.net': 'Яндекс', 'strm-rad-23.strm.yandex.net': 'Яндекс',
    'strm.yandex.net': 'Яндекс', 'strm.yandex.ru': 'Яндекс', 'log.strm.yandex.ru': 'Яндекс',
    'egress.yandex.net': 'Яндекс', 'cdnrhkgfkkpupuotntfj.svc.cdn.yandex.net': 'Яндекс',
    'csp.yandex.net': 'Яндекс', 'mail.ru': 'Mail.ru', 'e.mail.ru': 'Mail.ru', 'my.mail.ru': 'Mail.ru',
    'cloud.mail.ru': 'Mail.ru', 'inbox.ru': 'Mail.ru', 'list.ru': 'Mail.ru', 'bk.ru': 'Mail.ru',
    'myteam.mail.ru': 'Mail.ru', 'trk.mail.ru': 'Mail.ru', '1l-api.mail.ru': 'Mail.ru',
    'rutube.ru': 'Rutube', 'static.rutube.ru': 'Rutube', 'rutubelist.ru': 'Rutube',
    'pic.rutubelist.ru': 'Rutube', 'ssp.rutube.ru': 'Rutube', 'preview.rutube.ru': 'Rutube',
    'goya.rutube.ru': 'Rutube', 'smotrim.ru': 'Смотрим', 'ivi.ru': 'Ivi', 'cdn.ivi.ru': 'Ivi',
    'okko.tv': 'Okko', 'start.ru': 'Start', 'wink.ru': 'Wink', 'kion.ru': 'Kion',
    'premier.one': 'Premier', 'more.tv': 'More.tv', 'm.47news.ru': '47 News',
    'lenta.ru': 'Лента.ру', 'gazeta.ru': 'Газета.ру', 'kp.ru': 'Комсомольская Правда',
    'rambler.ru': 'Рамблер', 'ria.ru': 'РИА Новости', 'tass.ru': 'ТАСС',
    'interfax.ru': 'Интерфакс', 'kommersant.ru': 'Коммерсант', 'vedomosti.ru': 'Ведомости',
    'rbc.ru': 'РБК', 'russian.rt.com': 'RT', 'iz.ru': 'Известия', 'mk.ru': 'Московский Комсомолец',
    'rg.ru': 'Российская Газета', 'www.kinopoisk.ru': 'Кинопоиск', 'widgets.kinopoisk.ru': 'Кинопоиск',
    'payment-widget.plus.kinopoisk.ru': 'Кинопоиск', 'external-api.mediabilling.kinopoisk.ru': 'Кинопоиск',
    'external-api.plus.kinopoisk.ru': 'Кинопоиск', 'graphql-web.kinopoisk.ru': 'Кинопоиск',
    'graphql.kinopoisk.ru': 'Кинопоиск', 'tickets.widget.kinopoisk.ru': 'Кинопоиск',
    'st.kinopoisk.ru': 'Кинопоиск', 'quiz.kinopoisk.ru': 'Кинопоиск',
    'payment-widget.kinopoisk.ru': 'Кинопоиск', 'payment-widget-smarttv.plus.kinopoisk.ru': 'Кинопоиск',
    'oneclick-payment.kinopoisk.ru': 'Кинопоиск', 'microapps.kinopoisk.ru': 'Кинопоиск',
    'ma.kinopoisk.ru': 'Кинопоиск', 'hd.kinopoisk.ru': 'Кинопоиск',
    'crowdtest.payment-widget-smarttv.plus.tst.kinopoisk.ru': 'Кинопоиск',
    'crowdtest.payment-widget.plus.tst.kinopoisk.ru': 'Кинопоиск', 'api.plus.kinopoisk.ru': 'Кинопоиск',
    'st-im.kinopoisk.ru': 'Кинопоиск', 'sso.kinopoisk.ru': 'Кинопоиск', 'touch.kinopoisk.ru': 'Кинопоиск',
    '2gis.ru': '2ГИС', '2gis.com': '2ГИС', 'api.2gis.ru': '2ГИС', 'keys.api.2gis.com': '2ГИС',
    'tutu.ru': 'Туту.ру', 'img.tutu.ru': 'Туту.ру', 'rzd.ru': 'РЖД', 'ticket.rzd.ru': 'РЖД',
    'cdek.ru': 'СДЭК', 'cdek.market': 'СДЭК', 'calc.cdek.ru': 'СДЭК', 'pochta.ru': 'Почта России',
    'rostelecom.ru': 'Ростелеком', 'rt.ru': 'Ростелеком', 'mts.ru': 'МТС', 'megafon.ru': 'Мегафон',
    'beeline.ru': 'Билайн', 'tele2.ru': 'Tele2', 't2.ru': 'Tele2', 'www.t2.ru': 'Tele2',
    'msk.t2.ru': 'Tele2', 's3.t2.ru': 'Tele2', 'yota.ru': 'Yota', 'domru.ru': 'Дом.ру',
    'ertelecom.ru': 'ЭР-Телеком', 'selectel.ru': 'Selectel', 'timeweb.ru': 'Timeweb',
    'gismeteo.ru': 'Гисметео', 'meteoinfo.ru': 'Метео', 'rp5.ru': 'RП5', 'hh.ru': 'HeadHunter',
    'superjob.ru': 'SuperJob', 'rabota.ru': 'Работа.ру', 'auto.ru': 'Auto.ru',
    'sso.auto.ru': 'Auto.ru', 'drom.ru': 'Drom', 'avto.ru': 'Avto.ru', 'eda.ru': 'Eda.ru',
    'food.ru': 'Food.ru', 'edadeal.ru': 'Edadeal', 'delivery-club.ru': 'Delivery Club',
    'leroymerlin.ru': 'Леруа Мерлен', 'lemanapro.ru': 'Лемана Про', 'cdn.lemanapro.ru': 'Лемана Про',
    'static.lemanapro.ru': 'Лемана Про', 'dmp.dmpkit.lemanapro.ru': 'Лемана Про',
    'receive-sentry.lmru.tech': 'Лемана Про', 'partners.lemanapro.ru': 'Лемана Про',
    'petrovich.ru': 'Петрович', 'maxidom.ru': 'Максидом', 'vseinstrumenti.ru': 'ВсеИнструменты',
    '220-volt.ru': '220 Вольт', 'max.ru': 'Max', 'dev.max.ru': 'Max', 'web.max.ru': 'Max',
    'api.max.ru': 'Max', 'legal.max.ru': 'Max', 'st.max.ru': 'Max', 'botapi.max.ru': 'Max',
    'link.max.ru': 'Max', 'download.max.ru': 'Max', 'i.max.ru': 'Max', 'help.max.ru': 'Max',
    'mos.ru': 'Мос.ру', 'taximaxim.ru': 'Такси Максим', 'moskva.taximaxim.ru': 'Такси Максим'
}

print(f"📋 Загружено {len(DOMAIN_NAMES)} доменов в словарь")

# ========= ЛОГ =========
async def log(message: str):
    try:
        now = datetime.now()
        if os.path.exists(LOG_FILE):
            mtime = datetime.fromtimestamp(os.path.getmtime(LOG_FILE))
            if now - mtime > timedelta(seconds=LOG_CLEAN_INTERVAL):
                open(LOG_FILE, "w").close()

        async with aiofiles.open(LOG_FILE, "a", encoding="utf-8") as f:
            await f.write(f"[{now}] {message}\n")
    except:
        pass

def log_xray_error(message: str):
    try:
        with open(XRAY_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{datetime.now().strftime('%H:%M:%S')}] {message}\n")
    except:
        pass

# ========= ВАЛИДАЦИЯ =========
def validate_vless(url: str) -> bool:
    if not url.startswith("vless://"): return False
    if not UUID_REGEX.search(url): return False
    if "@" not in url or ":" not in url: return False
    return True

# ========= WHITELIST =========
def load_whitelist_domains():
    domains = set()
    suffixes = []
    if os.path.exists("whitelist.txt"):
        try:
            with open("whitelist.txt", "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    d = line.strip().lower()
                    if d:
                        domains.add(d)
                        suffixes.append("." + d)
            print(f"📋 Загружено {len(domains)} доменов из whitelist.txt")
        except:
            print("⚠️ Ошибка загрузки whitelist.txt")
    return domains, suffixes

# ========= ПРОТОКОЛ / SNI / НАЗВАНИЕ =========
def detect_protocol(vless_url: str) -> str:
    try:
        no_scheme = vless_url[len("vless://"):]
        after_at = no_scheme.split("@", 1)[1]
        query = after_at.split("?", 1)[1] if "?" in after_at else ""
        params = dict(urllib.parse.parse_qsl(query))

        transport = params.get("type", "").lower()
        security = params.get("security", "").lower()

        if transport in ("ws", "websocket"): return "WS"
        if transport in ("grpc", "gun"): return "gRPC"
        if transport in ("xhttp", "httpupgrade"): return "XHTTP"
        if transport in ("h2", "http2"): return "H2"
        if transport == "tcp": return "TCP"
        if security == "reality": return "Reality"
        if security in ("tls", "xtls"): return "TLS"
        return "TCP"
    except:
        return "Неизвестно"

def extract_all_possible_domains(vless_url: str) -> list:
    domains = set()
    try:
        if not vless_url.startswith("vless://"): return []
        content = vless_url[8:]
        at_pos = content.find('@')
        if at_pos == -1: return []
        
        after_at = content[at_pos+1:]
        q_pos = after_at.find('?')
        
        if q_pos != -1:
            host_part = after_at[:q_pos]
            query_part = after_at[q_pos+1:]
        else:
            host_part = after_at
            query_part = ""
        
        host = host_part.split(':', 1)[0]
        if host and '.' in host:
            domains.add(host.lower())
        
        if query_part:
            if '#' in query_part: query_part = query_part.split('#', 1)[0]
            params = dict(urllib.parse.parse_qsl(query_part))
            
            for k in ['sni', 'host']:
                if k in params and '.' in params[k]:
                    domains.add(params[k].lower())
                    
            if 'path' in params:
                path_parts = re.findall(r'[a-zA-Z0-9][a-zA-Z0-9\-\.]+[a-zA-Z0-9]\.[a-zA-Z]{2,}', params['path'])
                domains.update(d.lower() for d in path_parts)
                
        return list(domains)
    except:
        return []

def get_human_name(domain: str) -> str:
    if not domain: return "Неизвестно"
    d = domain.lower()
    
    if d in DOMAIN_NAMES: return DOMAIN_NAMES[d]
    
    parts = d.split('.')
    for i in range(len(parts) - 1):
        if ".".join(parts[i:]) in DOMAIN_NAMES:
            return DOMAIN_NAMES[".".join(parts[i:])]
            
    if len(parts) >= 2 and ".".join(parts[-2:]) in DOMAIN_NAMES:
        return DOMAIN_NAMES[".".join(parts[-2:])]
        
    return "Неизвестно"

def filter_by_sni(vless_url: str, whitelist_domains: set, whitelist_suffixes: list) -> bool:
    domains = extract_all_possible_domains(vless_url)
    
    if whitelist_domains:
        for domain in domains:
            if domain in whitelist_domains or any(domain.endswith(s) for s in whitelist_suffixes): return True
            if len(domain.split('.')) >= 2 and '.'.join(domain.split('.')[-2:]) in whitelist_domains: return True
        return False

    for domain in domains:
        if domain in DOMAIN_NAMES: return True
        parts = domain.split('.')
        for i in range(len(parts) - 1):
            if ".".join(parts[i:]) in DOMAIN_NAMES: return True
        if len(parts) >= 2 and ".".join(parts[-2:]) in DOMAIN_NAMES: return True
            
    return False

# ========= СКАЧИВАНИЕ =========
async def fetch(session, url, sem):
    async with sem:
        try:
            print(f"Скачиваю: {url}")
            async with session.get(url, timeout=15) as resp:
                if resp.status == 200:
                    return await resp.text()
        except Exception as e:
            await log(f"Ошибка при скачивании {url}: {e}")
    return None

async def process_url(session, url, sem, results_list, stats):
    content = await fetch(session, url, sem)
    stats["processed"] += 1

    if content:
        matches = VLESS_REGEX.findall(content)
        if matches:
            results_list.extend(matches)
            stats["found"] += len(matches)

    print(f"Обработано: {stats['processed']} | Найдено VLESS: {stats['found']}", end="\r")

# ========= ОЧИСТКА =========
async def clean_vless():
    print("\nОчищаю дубликаты и проверяю валидность...")
    if not os.path.exists(OUTPUT_FILE): return

    try:
        async with aiofiles.open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            lines = await f.readlines()
    except:
        return

    unique_valid = {url.strip() for url in lines if url.strip() and validate_vless(url.strip())}

    async with aiofiles.open(CLEAN_FILE, "w", encoding="utf-8") as f:
        await f.write("\n".join(unique_valid) + "\n")

    print(f"Очистка завершена. Итоговых конфигов: {len(unique_valid)}")

# ========= ФИЛЬТРАЦИЯ ПО WHITELIST =========
async def filter_vless():
    print("\n=== Фильтрация по whitelist ===")
    if not os.path.exists(CLEAN_FILE): return

    domains, suffixes = load_whitelist_domains()
    try:
        with open(CLEAN_FILE, "r", encoding="utf-8", errors="ignore") as f:
            total = sum(1 for _ in f)
    except:
        total = 0
        
    passed, processed = 0, 0

    async with aiofiles.open(CLEAN_FILE, "r", encoding="utf-8") as f_in, \
               aiofiles.open(FILTERED_FILE, "w", encoding="utf-8") as f_out:
        async for line in f_in:
            processed += 1
            url = line.strip()
            if not url: continue

            if filter_by_sni(url, domains, suffixes):
                await f_out.write(url + "\n")
                passed += 1

            if processed % 100 == 0 and total > 0:
                print(f"Фильтрация: {processed}/{total} | Подошло: {passed}", end="\r")

    print(f"\nФильтрация завершена. Итог: {passed} конфигов.")

# ========= ПЕРЕИМЕНОВАНИЕ =========
async def rename_configs():
    print("\n=== Переименование конфигов по протоколу и SNI ===")
    if not os.path.exists(FILTERED_FILE): return

    try:
        with open(FILTERED_FILE, "r", encoding="utf-8", errors="ignore") as f:
            total = sum(1 for _ in f)
    except:
        total = 0
        
    processed = 0

    async with aiofiles.open(FILTERED_FILE, "r", encoding="utf-8") as f_in, \
               aiofiles.open(NAMED_FILE, "w", encoding="utf-8") as f_out:
        async for line in f_in:
            processed += 1
            url = line.strip()
            if not url: continue

            protocol = detect_protocol(url)
            domains = extract_all_possible_domains(url)
            human_name = "Неизвестно"
            
            if domains:
                for domain in domains:
                    name = get_human_name(domain)
                    if name != "Неизвестно":
                        human_name = name
                        break
                if human_name == "Неизвестно" and domains:
                    human_name = domains[0].split('.')[-2].capitalize() if len(domains[0].split('.')) >= 2 else domains[0]

            # Использование V.O.I.D без дополнительных символов (чистый тег)
            title = f"{protocol}, {human_name} [V.O.I.D]"
            base = url.split("#", 1)[0]
            new_url = f"{base}#{title}"

            await f_out.write(new_url + "\n")

            if processed % 500 == 0 and total > 0:
                print(f"Переименовано: {processed}/{total}", end="\r")

    print(f"\nПереименование завершено. Итог: {processed} конфигов.")

# ========= НОРМАЛИЗАЦИЯ И КОДИРОВАНИЕ URL =========
def encode_vless_url(url: str) -> str:
    try:
        if not url.startswith("vless://"): return url
        
        content = url[8:]
        at_pos = content.find('@')
        if at_pos == -1: return url
        
        uuid = content[:at_pos]
        after_at = content[at_pos+1:]
        
        q_pos = after_at.find('?')
        if q_pos != -1:
            host_part = after_at[:q_pos]
            params_part = after_at[q_pos+1:]
        else:
            host_part = after_at
            params_part = ""
        
        hash_pos = host_part.find('#')
        if hash_pos != -1:
            host_only = host_part[:hash_pos]
            fragment = host_part[hash_pos+1:]
        else:
            host_only = host_part
            fragment = ""
        
        if not fragment and params_part:
            hash_pos = params_part.find('#')
            if hash_pos != -1:
                params_only = params_part[:hash_pos]
                fragment = params_part[hash_pos+1:]
                params_part = params_only
        
        params = dict(urllib.parse.parse_qsl(params_part))
        encoded_params = []
        for k, v in params.items():
            if k in ['security', 'type', 'fp', 'pbk', 'sid', 'flow']:
                encoded_params.append(f"{k}={v}")
            else:
                encoded_params.append(f"{k}={urllib.parse.quote(v, safe='')}")
        
        new_params = "&".join(encoded_params)
        
        try:
            encoded_fragment = urllib.parse.quote(fragment, safe='') if fragment and any(ord(c) > 127 for c in fragment) else fragment
        except:
            encoded_fragment = fragment
        
        base = f"vless://{uuid}@{host_only}"
        if new_params: base += f"?{new_params}"
        if encoded_fragment: base += f"#{encoded_fragment}"
        
        return base
        
    except Exception:
        return url

async def encode_all_configs():
    print("\n=== Кодирование конфигов для Xray ===")
    if not os.path.exists(NAMED_FILE): return
    
    try:
        with open(NAMED_FILE, 'r', encoding='utf-8') as f:
            configs = [line.strip() for line in f if line.strip()]
    except:
        return
    
    total = len(configs)
    changed = 0
    
    async with aiofiles.open(ENCODED_FILE, "w", encoding="utf-8") as f_out:
        for i, url in enumerate(configs, 1):
            encoded_url = encode_vless_url(url)
            await f_out.write(encoded_url + "\n")
            if encoded_url != url: changed += 1
            if i % 500 == 0:
                print(f"Закодировано: {i}/{total} | Изменено: {changed}", end="\r")
    
    print(f"\nКодирование завершено. Всего: {total}, изменено: {changed}")

# ========= АЛЬТЕРНАТИВНЫЕ МЕТОДЫ ПРОВЕРКИ =========
def check_tcp_connection(host: str, port: int, timeout: int = 2) -> bool:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except:
        return False

def check_tls_handshake(host: str, port: int = 443, sni: str = None, timeout: int = 2) -> tuple:
    try:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((host, port))
        
        ssl_sock = context.wrap_socket(sock, server_hostname=sni or host)
        ssl_sock.do_handshake()
        version = ssl_sock.version()
        ssl_sock.close()
        sock.close()
        return (True, version, None)
    except Exception as e:
        return (False, None, str(e))

# ========= XRAY-ТЕСТЕР =========
class SimpleProgress:
    def __init__(self, total):
        self.total = total
        self.current = 0
        self.start_time = time.time()
        self.lock = threading.Lock()
        self.working_count = 0
        self.retry_count = 0
    
    def update(self, status='', working=False, retry=False):
        with self.lock:
            self.current += 1
            if working: self.working_count += 1
            if retry: self.retry_count += 1
            if self.current % 10 == 0 or self.current == self.total:
                elapsed = time.time() - self.start_time
                speed = self.current / elapsed if elapsed > 0 else 0
                print(f"\r📊 [{self.current}/{self.total}] ✅:{self.working_count} 🔄:{self.retry_count} {speed:.1f} к/с {status}", end='', flush=True)
    
    def finish(self):
        elapsed = time.time() - self.start_time
        print(f"\r✅ Готово! {self.current} конфигов за {elapsed:.1f}с ({self.current/elapsed:.1f} к/с), рабочих: {self.working_count}, повторов: {self.retry_count}")

class PortManager:
    def __init__(self, start=20000, end=25000):
        self.ports = list(range(start, end + 1))
        self.used = set()
        self.lock = threading.Lock()
    
    def get_port(self):
        with self.lock:
            available = [p for p in self.ports if p not in self.used]
            if not available: return None
            port = random.choice(available)
            self.used.add(port)
            return port
    
    def release_port(self, port):
        with self.lock:
            self.used.discard(port)

class XrayTester:
    def __init__(self, input_file='url_encoded.txt', output_file='url_work.txt', max_workers=30):
        self.input_file = input_file
        self.output_file = output_file
        self.max_workers = max_workers
        self.test_url = XRAY_TEST_URL
        self.timeout = XRAY_TIMEOUT
        self.max_retries = MAX_RETRIES
        self.retry_delay = RETRY_DELAY
        self.xray_dir = Path('./xray_bin')
        self.xray_path = self.xray_dir / 'xray.exe'
        self.port_manager = PortManager()
        self.debug_file = DEBUG_FILE
        self.xray_log_file = XRAY_LOG_FILE
        
        print(f"🔍 XrayTester инициализирован")
        self.check_xray()

    def check_xray(self):
        if not self.xray_path.exists():
            print("⬇️ Скачиваю Xray...")
            self.download_xray()
        else:
            try:
                result = subprocess.run([str(self.xray_path), '-version'], capture_output=True, text=True, timeout=5)
                version = result.stdout.split('\n')[0] if result.stdout else 'Unknown'
                print(f"✅ Xray готов: {version}")
            except Exception as e:
                print(f"⚠️ Ошибка версии Xray: {e}")

    def download_xray(self):
        import urllib.request, zipfile
        self.xray_dir.mkdir(exist_ok=True)
        url = "https://github.com/XTLS/Xray-core/releases/latest/download/Xray-windows-64.zip"
        zip_path = self.xray_dir / "xray.zip"
        try:
            urllib.request.urlretrieve(url, zip_path)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(self.xray_dir)
            zip_path.unlink()
            print("✅ Xray загружен")
        except Exception as e:
            print(f"❌ Ошибка загрузки Xray: {e}")

    def parse_vless_url(self, url):
        try:
            if not url.startswith('vless://'): return None
            content = url[8:]
            at_pos = content.find('@')
            if at_pos == -1: return None
            
            uuid = content[:at_pos]
            after_at = content[at_pos+1:]
            
            q_pos = after_at.find('?')
            if q_pos != -1:
                host_part = after_at[:q_pos]
                query = after_at[q_pos+1:]
            else:
                host_part = after_at
                query = ""
            
            host_part = host_part.split('#')[0]
            host, port = (host_part.split(':', 1) + [443])[:2]
            
            params = dict(urllib.parse.parse_qsl(query))
            return {'uuid': uuid, 'host': host, 'port': int(port), 'params': params, 'url': url}
        except Exception:
            return None

    def create_xray_config(self, parsed, port):
        try:
            params = parsed['params']
            flow = params.get('flow', '')
            security = params.get('security', '')
            
            config = {
                "log": {"loglevel": "error"},
                "inbounds": [{"port": port, "protocol": "socks", "settings": {"auth": "noauth", "udp": False}, "tag": "socks-in"}],
                "outbounds": [{
                    "protocol": "vless",
                    "settings": {"vnext": [{"address": parsed['host'], "port": parsed['port'], "users": [{"id": parsed['uuid'], "encryption": "none", "flow": flow}]}]},
                    "streamSettings": {"network": params.get('type', 'tcp'), "security": security},
                    "tag": "proxy"
                }]
            }
            
            if security == 'reality':
                config["outbounds"][0]["streamSettings"]["realitySettings"] = {
                    "serverName": params.get('sni', parsed['host']), "fingerprint": params.get('fp', 'chrome'),
                    "publicKey": params.get('pbk', ''), "shortId": params.get('sid', ''), "spiderX": params.get('spx', '/')
                }
            elif security == 'tls':
                config["outbounds"][0]["streamSettings"]["tlsSettings"] = {"serverName": params.get('sni', parsed['host']), "allowInsecure": True}
            
            if params.get('type') in ('ws', 'websocket'):
                config["outbounds"][0]["streamSettings"]["wsSettings"] = {"path": params.get('path', '/'), "headers": {"Host": params.get('host', params.get('sni', parsed['host']))}}
            
            if params.get('type') in ('grpc', 'gun'):
                config["outbounds"][0]["streamSettings"]["grpcSettings"] = {"serviceName": params.get('servicename', params.get('service', '')), "multiMode": True}
            
            return config
        except Exception:
            return None

    def test_with_xray(self, parsed, port, attempt=1):
        config_file, process = None, None
        try:
            config = self.create_xray_config(parsed, port)
            if not config: return None
            
            fd, config_file = tempfile.mkstemp(suffix='.json')
            os.close(fd)
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f)
            
            creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
            process = subprocess.Popen([str(self.xray_path), '-c', config_file], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, creationflags=creationflags)
            
            time.sleep(1.0)
            if process.poll() is not None:
                return "CRASH"
            
            start = time.time()
            session = requests.Session()
            session.proxies = {'https': f'socks5://127.0.0.1:{port}'}
            
            try:
                r = session.get(self.test_url, timeout=self.timeout)
                if r.status_code in [200, 204]:
                    return {'working': True, 'ping': (time.time() - start) * 1000, 'method': 'xray'}
                return "FAIL"
            except requests.exceptions.Timeout: return "TIMEOUT"
            except requests.exceptions.ConnectionError: return "CONN_ERROR"
            except Exception: return "ERROR"
            
        except Exception:
            return "EXCEPTION"
        finally:
            if process:
                try: 
                    process.terminate()
                    process.wait(timeout=1.0)
                except: 
                    process.kill()
            if config_file and os.path.exists(config_file):
                try: os.remove(config_file)
                except: pass

    def check_alternative_methods(self, parsed, url):
        host, port = parsed['host'], parsed['port']
        params = parsed['params']
        if not check_tcp_connection(host, port, timeout=2): return None
        
        security = params.get('security', '')
        if security in ['reality', 'tls']:
            tls_ok, _, _ = check_tls_handshake(host, port, params.get('sni', host), timeout=2)
            if tls_ok:
                return {'url': url, 'ping': 100, 'method': 'tls_check', 'security': security}
        return None

    def test_one(self, url):
        parsed = self.parse_vless_url(url)
        if not parsed: return None
        
        port = self.port_manager.get_port()
        if port:
            try:
                for attempt in range(1, self.max_retries + 1):
                    result = self.test_with_xray(parsed, port, attempt)
                    if isinstance(result, dict) and result.get('working'):
                        self.port_manager.release_port(port)
                        return {'url': url, 'ping': result['ping'], 'method': 'xray'}
                    if result in ["TIMEOUT", "FAIL", "CONN_ERROR", "CRASH"] and attempt < self.max_retries:
                        time.sleep(self.retry_delay)
                        continue
                    break
            finally:
                self.port_manager.release_port(port)
        
        return self.check_alternative_methods(parsed, url)

    def test_all(self):
        if not os.path.exists(self.input_file): return
        
        try:
            with open(self.input_file, 'r', encoding='utf-8') as f:
                all_urls = [line.strip() for line in f if line.strip()]
        except:
            return
        
        if not all_urls: return
        
        print(f"\n🔍 Тестирование {len(all_urls)} конфигов")
        if os.path.exists(self.debug_file): os.remove(self.debug_file)
        
        working = []
        progress = SimpleProgress(len(all_urls))
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.test_one, url): url for url in all_urls}
            for future in as_completed(futures):
                try:
                    result = future.result(timeout=self.timeout + 5)
                    if result:
                        working.append(result)
                        progress.update('✅', working=True)
                    else:
                        progress.update('❌', working=False)
                except Exception:
                    progress.update('⚠️', working=False)
        
        progress.finish()
        working.sort(key=lambda x: x['ping'])
        
        with open(self.output_file, 'w', encoding='utf-8') as f:
            for w in working:
                f.write(w['url'] + '\n')
        
        print(f"\n📊 Результаты: ✅ Рабочих: {len(working)} | ❌ Не рабочих: {len(all_urls)-len(working)}\n")
        return working

    def run(self):
        self.test_all()

# ========= ОСНОВНОЙ ЦИКЛ =========
async def main_cycle():
    global cycle_counter
    cycle_counter += 1
    print(f"\n=== Новый цикл #{cycle_counter} ===")
    
    if cycle_counter % CYCLES_BEFORE_DEBUG_CLEAN == 0 and os.path.exists(DEBUG_FILE):
        os.remove(DEBUG_FILE)
    
    if os.path.exists(OUTPUT_FILE): os.remove(OUTPUT_FILE)
    if not os.path.exists(SOURCES_FILE): return

    try:
        with open(SOURCES_FILE, "r", encoding="utf-8", errors="ignore") as f:
            urls = [line.strip() for line in f if line.strip()]
    except:
        return

    if not urls: return
    print(f"📥 Загружаю {len(urls)} источников...")
    
    sem = asyncio.Semaphore(THREADS_DOWNLOAD)
    results_list = []
    stats = {"processed": 0, "found": 0}

    async with aiohttp.ClientSession() as session:
        tasks = [process_url(session, url, sem, results_list, stats) for url in urls]
        await asyncio.gather(*tasks)

    # Пакетная запись после завершения парсинга
    if results_list:
        async with aiofiles.open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            await f.write("\n".join(results_list) + "\n")

    print(f"\n✅ Скачивание завершено. Найдено VLESS: {stats['found']}")
    await log(f"Скачивание завершено. Найдено VLESS: {stats['found']}")

    if stats['found'] > 0:
        await clean_vless()
        await filter_vless()
        await rename_configs()
        await encode_all_configs()

        print("\n=== Запуск Xray-проверки ===")
        tester = XrayTester(input_file=ENCODED_FILE, output_file=WORK_FILE, max_workers=XRAY_MAX_WORKERS)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, tester.run)
    else:
        print("⏭️ Нет новых конфигов для обработки")

async def run_forever():
    print("\n🔄 Запуск бесконечного цикла...")
    while True:
        try:
            cycle_start = time.time()
            await main_cycle()
            print(f"✅ Цикл завершен за {time.time() - cycle_start:.1f}с")
            await asyncio.sleep(CYCLE_DELAY)
        except KeyboardInterrupt:
            break
        except Exception as e:
            await asyncio.sleep(60)

if __name__ == "__main__":
    try:
        asyncio.run(run_forever())
    except KeyboardInterrupt:
        print("\n👋 Программа остановлена")
