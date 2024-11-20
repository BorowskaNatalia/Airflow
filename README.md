https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

### **Zadanie z Airflow – Łącznie 20 punktów**

#### **Opis projektu**

Stwórz dwa przepływy zadań (**DAG**) w Airflow, które będą wykonywać następujące zadania związane z przetwarzaniem danych. Pierwszy DAG zajmuje się pobraniem i podziałem danych, a drugi przetwarza dane w celu przygotowania ich do dalszej analizy.

#### **Wymagania wstępne**

1. Zainstalowany **Apache Airflow** na lokalnym środowisku.
2. Zainstalowane biblioteki do obsługi danych w Pythonie, takie jak **pandas**, **scikit-learn**.
3. Konto w chmurze Google, aby zapisać podzielone zbiory danych w Google Sheets lub w inne miejsce na chmurze.

---

### **DAG 1: Pobranie i podział danych**
**(Maksymalnie 10 punktów)**

**Cel:** Utwórz DAG, który:
1. Pobiera plik z danymi (np. z linku lub z lokalnej ścieżki).
2. Dzieli dane na dwa zbiory:
   - **70% danych**: zbiór modelowy.
   - **30% danych**: zbiór do ewentualnego douczenia.
3. Zapisuje oba zbiory do osobnych arkuszy Google Sheets lub chmury (np. "Zbiór modelowy" i "Zbiór douczeniowy").

#### **Kroki do wykonania**
1. **Operator do pobrania danych (2 pkt)**  
   - Stwórz task, który pobiera dane z określonego źródła (np. plik `.csv` w lokalnym systemie plików).

2. **Operator do podziału danych (4 pkt)**  
   - Podziel dane na zbiór modelowe (70%) i douczeniowe (30%) przy użyciu **`train_test_split`** z biblioteki `scikit-learn`.
   - Upewnij się, że dane są losowo dzielone i że można ustawić losowość podziału za pomocą `random_state`.

3. **Operator do zapisu danych do Google Sheets (4 pkt)**  
   - Wgraj dwa zbiory danych (treningowy i testowy) do osobnych arkuszy Google Sheets.
   - Skonfiguruj autoryzację OAuth 2.0 lub użyj konta usługi (service account), aby móc zapisywać dane w Google Sheets.

> **Wskazówka:** Możesz użyć biblioteki `gspread` do pracy z Google Sheets w Pythonie lub innej, którą uważasz za wygodną.

---

### **DAG 2: Przetwarzanie danych**
**(Maksymalnie 10 punktów)**

**Cel:** Utwórz drugi DAG, który przetwarza dane z Google Sheets. Przetwarzanie obejmuje:
1. Czyszczenie danych – usunięcie wartości brakujących lub ich odpowiednie przetworzenie.
2. Standaryzację i normalizację danych.

#### **Kroki do wykonania**
1. **Operator do pobrania danych z Google Sheets lub innego źródła (2 pkt)**  
   - Stwórz task, który pobiera zbior modelowy zapisane w Google Sheets w DAG-u 1.

2. **Operator do czyszczenia danych (2 pkt)**  
   - Zidentyfikuj i usuń lub przetwórz brakujące wartości.
   - Dodatkowo, sprawdź, czy nie ma duplikatów, i w razie potrzeby usuń je.

3. **Operator do standaryzacji i normalizacji (4 pkt)**  
   - Standaryzacja: skaluj wartości cech.
   - Normalizacja: przeskaluj wartości cech na numery.

4. **Operator do czyszczenia danych (2 pkt)**  
   - Umieszczenie danych w chmurze lub Google Sheets

> **Wskazówka:** Możesz użyć `StandardScaler` i `MinMaxScaler` z biblioteki **scikit-learn**.

---

### **Dodatkowe informacje**

- **Dokumentacja:** Każdy DAG powinien być opisany w komentarzach, a kroki pracy udokumentowane, aby osoba sprawdzająca mogła zrozumieć podejście.
- **Testowanie:** Upewnij się, że oba DAG-i są poprawnie skonfigurowane i uruchamiają się w Airflow bez błędów.
- **Ocena końcowa** oparta będzie na działaniu i poprawności każdego etapu.

#### **Wynik zadania**

Wynikiem zadania ma być link do waszych dagów oraz screeny, że działją

---

Powodzenia!
