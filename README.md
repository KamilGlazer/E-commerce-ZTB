# E-commerce Database Benchmark (ZTB)

Projekt z przedmiotu Zaawansowane Technologie Bazodanowe (ZTB) demonstrujący działanie, wydajność i modelowanie danych w czterech wiodących silnikach bazodanowych. Środowisko opiera się na tematyce platformy E-commerce.

Wykorzystane bazy danych (uruchamiane jako kontenery Docker):
1. **PostgreSQL** (Relacyjna baza danych) - port `5432`
2. **MariaDB** (Relacyjna baza danych) - port `3306`
3. **MongoDB** (Dokumentowa baza NoSQL) - port `27017`
4. **Neo4j** (Grafowa baza danych) - port `7474` (skaner WWW) i `7687` (Bolt)

Sercem projektu jest generator danych napisany w Pythonie, który równolegle wypełnia w pełni znormalizowane (nawet dla baz NoSQL jako anty-wzorzec badawczy) środowiska potężnymi, relacyjnymi zestawami obciążeniowymi (np. 10 tysięcy, 100 tysięcy, 1 milion rekordów docelowych).

## Wymagania

Przed uruchomieniem projektu upewnij się, że masz zainstalowane na komputerze:
1. **Docker** oraz narzędzie **Docker Compose** (np. za pośrednictwem _Docker Desktop_).
2. **Python 3.8+** (rekomendowane w systemie Windows jest wywołanie polecenia `python`)

## Jak uruchomić projekt?

### Krok 1: Inicjalizacja baz danych

Przejdź do głównego katalogu projektu, pobierz obrazy i zainicjalizuj lokalne środowisko Docker:

```bash
docker-compose up -d
```
_Poczekaj około 15-30 sekund przy pierwszym uruchomieniu, aby silniki poprawnie przemieliły pliki z folderu `init-scripts/` i wystawiły porty._

### Krok 2: Konfiguracja Pythona (Środowisko Wirtualne)

Zalecane jest utworzenie środowiska wirtualnego dla bibliotek ładujących. 
W terminalu katalogu projektu uruchom:

```bash
# Utworzenie i aktywacja środowiska wirtualnego w systemie Windows:
python -m venv venv
.\venv\Scripts\activate

# Instalacja niezbędnych bibliotek sterowników oraz generatora danych:
pip install -r requirements.txt
```

### Krok 3: Generowanie Danych E-commerce

Gdy bazy są aktywne, uruchom poniższy skrypt. Skrypt jako jedyny parametr pobiera bazową liczbę **N**, od której kalkulowana jest proporcja generowanych encji w sklepie.

```bash
# Wygeneruje np. 10 tysięcy głównych encji (użytkowników/produktów)
# oraz x2 / x4 pozostałych encji z nimi powiązanych.
python seed.py 10000
```
> **Uwaga**: Ładowanie danych np. miliona rekordów w powtórzeniach `batch_insert` może zająć od kliku minut do dłuższej chwili. W konsoli dostępny jest wygodny pasek postępu (Tqdm).

## ⚠️ Bardzo ważne: Jak resetować środowisko do zera?

Skrypt ładujący polega na identyfikatorach klucza głównego (ID). Użytkownik startuje z ID = 1.
Jeżeli po teście na 10 tysiącach rekordów, zapragniesz przetestować 1 milion rekordów, absolutnie i bezwzględnie musisz zresetować wolumeny z danymi Dockera – tak aby uruchomić test na **idealnie czystych środowiskach bazodanowych!** 

Wyłącz i wyczyść wszystkie utrwalone na Twoim dysku (w lokalnym folderze `data/`) dane następującym poleceniem:

```bash
docker-compose down -v
rm -r data/    # lub ręcznie usuń ten folder z eksploratora plików Windows
```

Po usunięciu starych wolumenów wystarczy, że wystartujesz skrypt od nowa na idealnie czystych środowiskach: `docker-compose up -d` i ponownie włączysz swój skrypt `seed.py`.
