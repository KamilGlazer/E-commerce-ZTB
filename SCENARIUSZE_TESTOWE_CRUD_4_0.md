# Scenariusze testowe CRUD (poziom 4.0)

Dokument zawiera 24 zaawansowane scenariusze testowe dla projektu `E-commerce-ZTB`, zgodne z wymaganiem poziomu 4.0: dokładnie 6 scenariuszy dla każdej operacji CRUD.

## Założenia wspólne

- Testowane silniki: PostgreSQL, MariaDB, MongoDB, Neo4j.
- Profile danych: `small`, `medium`, `large` (zgodnie z `seed.py`).
- Każdy scenariusz wykonuj w 3 próbach i licz średnią czasu.
- Dla scenariuszy odczytu i części modyfikacji wykonaj pomiar:
  - przed utworzeniem indeksu,
  - po utworzeniu indeksu.
- Dla relacyjnych baz dokumentuj plan zapytania (`EXPLAIN` / `EXPLAIN ANALYZE`), dla MongoDB `explain("executionStats")`, dla Neo4j `PROFILE`.
- Po każdej serii testów przywracaj spójny stan danych (rollback danych testowych albo reset środowiska).

## CREATE (6 scenariuszy)

### C1. Masowe dodanie użytkowników (batch insert)
- Cel: porównać koszt czystego insertu bez relacji.
- Dane wejściowe: 50 000 nowych użytkowników (`users` / `User`).
- Kroki:
  1. Wygeneruj unikalny zakres ID poza istniejącym.
  2. Wstaw rekordy batchami po 1k, 5k i 10k.
  3. Zmierz całkowity czas operacji.
- Metryki: `elapsed_ms`, throughput (rek/s), błąd unikalności.
- Oczekiwany rezultat: wszystkie rekordy zapisane, brak duplikatów kluczy.

### C2. Dodanie zamówienia z 5 pozycjami (transakcja wielotabelowa)
- Cel: zbadać koszt zapisu encji nadrzędnej i zależnych.
- Dane wejściowe: 10 000 zamówień, każde z 5 `order_items`.
- Kroki:
  1. Rozpocznij transakcję.
  2. Dodaj `orders`, następnie `order_items`.
  3. Commit i pomiar czasu całej transakcji.
- Metryki: czas transakcji, liczba rollbacków, rekordy/s.
- Oczekiwany rezultat: pełna atomowość (brak osieroconych pozycji).

### C3. Dodanie recenzji z walidacją referencyjną
- Cel: sprawdzić narzut walidacji relacji `user-product`.
- Dane wejściowe: 100 000 recenzji dla istniejących produktów i użytkowników.
- Kroki:
  1. Losuj pary `product_id` i `user_id`.
  2. Zapisz recenzje (`rating`, `comment`, `created_at`).
  3. Mierz czas i odsetek odrzuceń.
- Metryki: czas zapisu, liczba błędów FK/constraint, skuteczność zapisu.
- Oczekiwany rezultat: zapisy tylko dla poprawnych referencji.

### C4. Dodanie koszyka i pozycji koszyka dla aktywnego użytkownika
- Cel: test przepływu write-heavy na strukturze koszyka.
- Dane wejściowe: 20 000 koszyków, każdy z 10 `cart_items`.
- Kroki:
  1. Dodaj `cart` dla użytkownika.
  2. Dodaj 10 pozycji z losowymi produktami.
  3. Sprawdź spójność liczby pozycji.
- Metryki: czas utworzenia koszyka, czas dodania pozycji, błędy duplikatów.
- Oczekiwany rezultat: każdy koszyk ma dokładnie 10 pozycji.

### C5. Dodanie płatności i wysyłki do istniejącego zamówienia
- Cel: ocenić koszt tworzenia rekordów "post-order".
- Dane wejściowe: 60 000 płatności i 60 000 wysyłek.
- Kroki:
  1. Dla istniejącego `order_id` dodaj `payment`.
  2. Dodaj `shipment` z unikalnym `tracking_number`.
  3. Zweryfikuj relacje do zamówienia.
- Metryki: czas insertu pojedynczego i batchowego, konflikty unikalności.
- Oczekiwany rezultat: wszystkie wpisy powiązane z zamówieniami.

### C6. Insert konfliktowy (duplikat klucza) i obsługa błędów
- Cel: przetestować koszt i poprawność ścieżki błędu.
- Dane wejściowe: 10 000 prób insertu z celowo zdublowanym ID.
- Kroki:
  1. Wstaw rekord poprawny.
  2. Powtórz insert z tym samym kluczem.
  3. Zmierz czas i zachowanie transakcji.
- Metryki: czas odpowiedzi na błąd, liczba poprawnych rollbacków.
- Oczekiwany rezultat: błąd constraint bez uszkodzenia danych.

## READ (6 scenariuszy)

### R1. Odczyt produktu po kluczu głównym (point lookup)
- Cel: porównać odczyt O(1)-like dla indeksu klastrowego/unikalnego.
- Dane wejściowe: 100 000 losowych `product_id`.
- Kroki:
  1. Wykonaj odczyt po `id` bez dodatkowych filtrów.
  2. Zanotuj plan (`EXPLAIN`/`PROFILE`).
  3. Powtórz 3 razy dla każdego profilu danych.
- Metryki: średni czas zapytania, p95 latency.
- Oczekiwany rezultat: stabilny czas i wykorzystanie indeksu ID.

### R2. Odczyt zamówień użytkownika po `user_id` (przed/po indeksie)
- Cel: zbadać wpływ indeksu na filtrowanie kolumny niekluczowej.
- Dane wejściowe: zapytanie po `orders.user_id` (duża kardynalność).
- Kroki:
  1. Uruchom zapytanie bez indeksu pomocniczego.
  2. Dodaj indeks na `user_id`.
  3. Powtórz pomiar i porównaj plan wykonania.
- Metryki: czas przed/po, liczba skanowanych rekordów.
- Oczekiwany rezultat: wyraźny spadek czasu po indeksowaniu.

### R3. JOIN `orders` + `users` + `payments`
- Cel: ocenić koszt wielotabelowego odczytu transakcyjnego.
- Dane wejściowe: 50 000 rekordów wynikowych.
- Kroki:
  1. Wykonaj zapytanie JOIN/$lookup/traversal dla 3 encji.
  2. Zmierz czas i liczność wyniku.
  3. Dla baz relacyjnych sprawdź plan joinów.
- Metryki: czas, liczba wierszy, operator join (nested/hash/merge).
- Oczekiwany rezultat: poprawny wynik i przewidywalny plan.

### R4. Agregacja sprzedaży wg statusu i metody płatności
- Cel: porównać wydajność agregacji grupującej.
- Dane wejściowe: wszystkie `orders` + `payments`.
- Kroki:
  1. Uruchom grupowanie `GROUP BY status, method`.
  2. Zmierz czas i poprawność sum/średnich.
  3. Powtórz po dodaniu indeksów wspierających filtry.
- Metryki: czas agregacji, memory usage (jeśli dostępne), liczba grup.
- Oczekiwany rezultat: poprawne agregaty i krótszy czas po indeksie.

### R5. Odczyt paginowany listy produktów (sort + limit)
- Cel: zbadać wpływ sortowania i paginacji na dużym zbiorze.
- Dane wejściowe: stronicowanie po `price` i `id`.
- Kroki:
  1. Odczytaj strony 1, 100, 1000 (stały `LIMIT`).
  2. Wykonaj test bez indeksu złożonego.
  3. Dodaj indeks (`price`, `id`) i porównaj.
- Metryki: czas każdej strony, koszt sortowania.
- Oczekiwany rezultat: mniejszy narzut dla dalekich stron po indeksie.

### R6. Odczyt grafowy ścieżki klient -> zamówienie -> produkt
- Cel: porównać traversal grafowy z odpowiednikiem relacyjnym.
- Dane wejściowe: 10 000 użytkowników, ich zamówienia i pozycje.
- Kroki:
  1. W Neo4j uruchom traversal `User-PLACED-Order-HAS_ITEM-FOR_PRODUCT`.
  2. W SQL/Mongo uruchom równoważne złączenia.
  3. Porównaj czas i kompletność wyniku.
- Metryki: czas, liczba zwróconych relacji, koszt operatorów.
- Oczekiwany rezultat: poprawna ścieżka danych i porównywalny wynik biznesowy.

## UPDATE (6 scenariuszy)

### U1. Aktualizacja stanu magazynowego produktu (hot update)
- Cel: sprawdzić koszt częstych aktualizacji pojedynczego pola.
- Dane wejściowe: 200 000 operacji `stock = stock - x`.
- Kroki:
  1. Wylosuj produkty i aktualizuj stan.
  2. Kontroluj, by `stock` nie spadał poniżej zera.
  3. Mierz czas i kolizje aktualizacji.
- Metryki: czas update, deadlock/conflict rate, spójność wartości.
- Oczekiwany rezultat: brak ujemnych stanów i akceptowalny czas.

### U2. Zmiana statusu zamówień wsadowo (PENDING -> SHIPPED)
- Cel: ocenić wydajność update masowego z warunkiem.
- Dane wejściowe: 300 000 zamówień `PENDING`.
- Kroki:
  1. Wykonaj update warunkowy bez indeksu na `status`.
  2. Dodaj indeks na `status`.
  3. Powtórz i porównaj czas/plan.
- Metryki: czas przed/po, liczba zmodyfikowanych rekordów.
- Oczekiwany rezultat: przyspieszenie filtrowania po indeksie.

### U3. Aktualizacja ceny całej kategorii produktów
- Cel: test update wielowierszowego po `category_id`.
- Dane wejściowe: jedna kategoria z dużą liczbą produktów.
- Kroki:
  1. Podnieś cenę o 5%.
  2. Cofnij zmianę (rollback testowy albo update odwracający).
  3. Zweryfikuj poprawność cen po obu krokach.
- Metryki: czas aktualizacji, liczba rekordów, blokady.
- Oczekiwany rezultat: pełna i odwracalna zmiana cen.

### U4. Aktualizacja recenzji użytkownika (treść + ocena)
- Cel: porównać update po kluczu złożonym/logice biznesowej.
- Dane wejściowe: 100 000 recenzji (po `user_id` + `product_id`).
- Kroki:
  1. Aktualizuj ocenę i komentarz.
  2. Wykonaj pomiar bez i z indeksem złożonym.
  3. Sprawdź zgodność liczby aktualizacji.
- Metryki: czas, liczba trafionych rekordów, plan użycia indeksu.
- Oczekiwany rezultat: szybszy update po indeksie kompozytowym.

### U5. Aktualizacja płatności po statusie niepowodzenia (retry flow)
- Cel: odwzorować realny przepływ biznesowy "ponów płatność".
- Dane wejściowe: rekordy `payments` ze statusem `FAILED`.
- Kroki:
  1. Zmień `status` na `SUCCESS` dla poprawnych retry.
  2. Aktualizuj `payment_date`.
  3. Zweryfikuj spójność z zamówieniem.
- Metryki: czas update, liczba poprawionych płatności.
- Oczekiwany rezultat: tylko kwalifikujące się rekordy zmieniają status.

### U6. Współbieżna aktualizacja tego samego zamówienia
- Cel: sprawdzić odporność na race condition.
- Dane wejściowe: 2-3 równoległe procesy aktualizujące `orders.status`.
- Kroki:
  1. Uruchom równoległe update na tym samym `order_id`.
  2. Rejestruj konflikty/ponowienia.
  3. Zweryfikuj końcowy, jednolity stan.
- Metryki: czas, liczba konfliktów, liczba retry.
- Oczekiwany rezultat: brak utraconych aktualizacji.

## DELETE (6 scenariuszy)

### D1. Usunięcie pojedynczego koszyka z pozycjami (cascade)
- Cel: zweryfikować poprawność i koszt kasowania zależności.
- Dane wejściowe: `cart` z 10+ `cart_items`.
- Kroki:
  1. Usuń koszyk.
  2. Zweryfikuj usunięcie rekordów potomnych.
  3. Powtórz dla 10 000 koszyków.
- Metryki: czas delete, liczba osieroconych rekordów.
- Oczekiwany rezultat: pełna kaskada bez sierot.

### D2. Masowe usuwanie starych recenzji
- Cel: sprawdzić wydajność delete warunkowego po dacie.
- Dane wejściowe: recenzje starsze niż ustalona data graniczna.
- Kroki:
  1. Usuń rekordy bez indeksu na `created_at`.
  2. Odtwórz dane testowe.
  3. Dodaj indeks i porównaj czas.
- Metryki: czas przed/po, liczba usuniętych rekordów.
- Oczekiwany rezultat: krótszy czas selekcji rekordów do usunięcia.

### D3. Usuwanie porzuconych zamówień (status CANCELLED)
- Cel: test delete biznesowego dla dużych wolumenów.
- Dane wejściowe: zamówienia `CANCELLED` + powiązane byty.
- Kroki:
  1. Usuń zamówienia partiami (np. po 10k).
  2. Kontroluj integralność (`order_items`, `payments`, `shipments`).
  3. Zmierz czas dla różnych rozmiarów batchy.
- Metryki: czas, liczba błędów integralności, wpływ rozmiaru batcha.
- Oczekiwany rezultat: brak naruszeń integralności.

### D4. Usunięcie użytkownika z pełnym drzewem zależności
- Cel: ocenić najcięższy przypadek kasowania encji głównej.
- Dane wejściowe: użytkownik z zamówieniami, recenzjami i koszykiem.
- Kroki:
  1. Usuń użytkownika z zachowaniem reguł biznesowych.
  2. Sprawdź dane zależne po kasowaniu.
  3. Zmierz czas operacji.
- Metryki: czas, kompletność usunięcia, liczba wyjątków.
- Oczekiwany rezultat: brak osieroconych danych użytkownika.

### D5. Delete konfliktowy (naruszenie ograniczeń)
- Cel: przetestować obsługę błędu przy braku kaskady.
- Dane wejściowe: próba usunięcia `product` używanego w `order_items`.
- Kroki:
  1. Wykonaj delete encji nadrzędnej.
  2. Oczekuj błędu constraint/protect.
  3. Zweryfikuj, że dane pozostają niezmienione.
- Metryki: czas odpowiedzi błędu, stan danych po błędzie.
- Oczekiwany rezultat: operacja odrzucona, brak częściowego usunięcia.

### D6. Soft delete vs hard delete produktów
- Cel: porównać koszt i wpływ na odczyty.
- Dane wejściowe: 100 000 produktów testowych.
- Kroki:
  1. Wykonaj soft delete (flaga `is_deleted=true` albo odpowiednik).
  2. Wykonaj hard delete na tej samej skali (na odtworzonych danych).
  3. Porównaj czasy delete oraz późniejsze czasy odczytu katalogu.
- Metryki: czas operacji, wpływ na zapytania read, rozmiar danych.
- Oczekiwany rezultat: świadomy wybór strategii kasowania do raportu.

## Minimalny format raportowania dla każdego scenariusza

- Identyfikator scenariusza (`C1`...`D6`).
- Silnik bazodanowy.
- Rozmiar danych (`small`/`medium`/`large`).
- Konfiguracja indeksów (przed/po).
- 3 czasy prób i średnia.
- Plan zapytania / plan wykonania.
- Krótki wniosek (2-3 zdania).

