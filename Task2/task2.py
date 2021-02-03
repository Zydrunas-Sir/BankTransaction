from Task2.DatabaseManager import DatabaseContextManager


def create_table_transactions():
    query = """CREATE TABLE `transactions` (
    `id` integer NOT NULL AUTO_INCREMENT,
    `from_account_id` integer,
    `to_account_id` integer,
    `amount` float,
    PRIMARY KEY (id),
    FOREIGN KEY (from_account_id) REFERENCES account(id),
    FOREIGN KEY (to_account_id) REFERENCES account(id));"""
    with DatabaseContextManager() as db:
        cursor = db.cursor()
        cursor.execute(query)


def create_table_account():
    query = """CREATE TABLE `account` (
    `id` integer NOT NULL AUTO_INCREMENT,
    `first_name` varchar(255),
    `last_name` varchar(255),
    `balance` float,
    PRIMARY KEY (id));"""
    with DatabaseContextManager() as db:
        cursor = db.cursor()
        cursor.execute(query)


def transaction(from_account_id, to_account_id, amount):
    check_balance_sql = """ Select balance from account Where id = %s
                """
    check_balance_param = [from_account_id]
    update_account_from_sql = """ Update account
                set balance = balance - %s
                Where id = %s
                """
    update_account_from_param = [amount, from_account_id]
    update_account_to_sql = """ Update account
                set balance = balance + %s
                Where id = %s
                """
    update_account_to_param = [amount, to_account_id]
    check_account_to_sql = """Select id from account where id = %s"""
    check_account_to_param = [to_account_id]
    create_transaction_sql = f"""INSERT INTO transactions(from_account_id, to_account_id, amount) VALUES(%s, %s, %s)"""
    create_transaction_param = [from_account_id, to_account_id, amount]
    with DatabaseContextManager() as db:
        cursor = db.cursor()
        try:
            cursor.execute(check_balance_sql, check_balance_param)
            if cursor.fetchone()[0] < amount:
                raise Exception
            cursor.execute(check_account_to_sql, check_account_to_param)
            if cursor.fetchone() is None:
                raise Exception
            cursor.execute(update_account_from_sql, update_account_from_param)
            cursor.execute(update_account_to_sql, update_account_to_param)
            cursor.execute(create_transaction_sql, create_transaction_param)
        except Exception as error:
            print("Failed to update record to database rollback: {}".format(error))
            db.rollback()


def create_account(first_name: str, last_name: str, balance: float):
    query = f"""INSERT INTO account(first_name, last_name, balance) VALUES(%s, %s, %s)"""
    parameters = [first_name, last_name, balance]
    with DatabaseContextManager() as db:
        cursor = db.cursor()
        cursor.execute(query, parameters)
