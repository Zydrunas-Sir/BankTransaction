from Task2.DatabaseManager import DatabaseContextManager


def create_table_transactions():
    query = """CREATE TABLE `transactions` (
    `id` integer,
    `from_account_id` integer,
    `to_account_id` integer,
    `amount` decimal(5, 2),
    PRIMARY KEY (id),
    FOREIGN KEY (from_account_id) REFERENCES transactions(id),
    FOREIGN KEY (to_account_id) REFERENCES transactions(id));"""
    with DatabaseContextManager() as db:
        cursor = db.cursor()
        cursor.execute(query)


def create_table_account():
    query = """CREATE TABLE `account` (
    `id` integer,
    `first_name` varchar(255),
    `last_name` varchar(255),
    `balance` decimal(5, 2),
    PRIMARY KEY (id));"""
    with DatabaseContextManager() as db:
        cursor = db.cursor()
        cursor.execute(query)


def transaction():
    try:
        query = """ Update account set account.balance =- transactions.amount
                where account.id = transactions.from_account_id
                and account.id is not null and transactions.from_account_id is not null
                """
        query1 = """ Update account set account.balance =+ transactions.amount
                where account.id = transactions.to_account_id
                and account.id is not null and transactions.to_account_id is not null
                """
        with DatabaseContextManager() as db:
            cursor = db.cursor()
            cursor.execute(query, query1)

    except cursor.connector.Error as error:
        print("Failed to update record to database rollback: {}".format(error))
        db.rollback()
    finally:
        if db.is_connected():
            db.close()
            print("connection is closed")


def create_transaction(from_account_id: int, to_account_id: int, amount: float):
    query = f"""INSERT INTO transactions(from_account_id, to_account_id, amount) VALUES(?, ?, ?)"""
    parameters = [from_account_id, to_account_id, amount]
    with DatabaseContextManager("db") as db:
        cursor = db.cursor()
        cursor.execute(query, parameters)


def create_account(first_name: str, last_name: str, balance: float):
    query = f"""INSERT INTO account(first_name, last_name, balance) VALUES(?, ?, ?)"""
    parameters = [first_name, last_name, balance]
    with DatabaseContextManager("db") as db:
        cursor = db.cursor()
        cursor.execute(query, parameters)
