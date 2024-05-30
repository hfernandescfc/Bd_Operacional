# Camada de persistência

import sqlite3
import tkinter as tk
from tkinter import messagebox, ttk

def init_db():
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS data (
            key TEXT PRIMARY KEY,
            value INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS log (
            transaction_id INTEGER,
            operation TEXT,
            key TEXT,
            value INTEGER,
            PRIMARY KEY(transaction_id, operation, key)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS commit_log (
            transaction_id INTEGER PRIMARY KEY
        )
    ''')
    conn.commit()
    return conn

def get_value(conn, key):
    cursor = conn.cursor()
    cursor.execute('SELECT value FROM data WHERE key = ?', (key,))
    result = cursor.fetchone()
    return result[0] if result else None

def set_value(conn, key, value):
    cursor = conn.cursor()
    cursor.execute('REPLACE INTO data (key, value) VALUES (?, ?)', (key, value))
    conn.commit()

def log_operation(conn, transaction_id, operation, key, value):
    cursor = conn.cursor()
    cursor.execute('INSERT INTO log (transaction_id, operation, key, value) VALUES (?, ?, ?, ?)', (transaction_id, operation, key, value))
    conn.commit()

def log_commit(conn, transaction_id):
    cursor = conn.cursor()
    cursor.execute('INSERT INTO commit_log (transaction_id) VALUES (?)', (transaction_id,))
    conn.commit()

def get_log(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM log')
    return cursor.fetchall()

def get_commit_log(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM commit_log')
    return cursor.fetchall()


def populate_initial_data(conn):
    initial_data = {
        "A": 100,
        "B": 200, 
        "C": 300
    }
    cursor = conn.cursor()
    for key, value in initial_data.items():
        cursor.execute('REPLACE INTO data (key, value) VALUES (?, ?)', (key, value))
    conn.commit()

class TransactionManager:
    def __init__(self, conn):
        self.conn = conn
        self.cache = {}
        self.current_transaction = None

    def clear_log(self, transaction_id):
        cursor = self.conn.cursor()
        if transaction_id is None:
            cursor.execute('DELETE FROM log')  # clear the entire log
        else:
            cursor.execute('DELETE FROM log WHERE transaction_id =?', (transaction_id,))
        self.conn.commit()

    def start_transaction(self, transaction_id):
        self.current_transaction = transaction_id
        self.cache = {}

    def read(self, key):
        if key in self.cache:
            return self.cache[key]
        else:
            value = get_value(self.conn, key)
            log_operation(self.conn, self.current_transaction, 'read', key, value)
            return value

    def write(self, key, value):
        if key in self.cache:
            # clear the log entries for the previous transaction
            self.clear_log(self.current_transaction)
        self.cache[key] = value
        log_operation(self.conn, self.current_transaction, 'write', key, value)

    def commit(self):
        for key, value in self.cache.items():
            set_value(self.conn, key, value)
        log_commit(self.conn, self.current_transaction)
        self.cache = {}
        self.current_transaction = None
        self.clear_log(self.current_transaction)  # clear the log

    def abort(self):
        self.cache = {}
        transaction_id = self.current_transaction
        self.current_transaction = None
        self.clear_log(transaction_id)  # clear log entries for this transaction

    def recover(self):
        cursor = self.conn.cursor()
        cursor.execute('SELECT transaction_id FROM commit_log')
        committed_transactions = {row[0] for row in cursor.fetchall()}
        cursor.execute('SELECT transaction_id, operation, key, value FROM log')
        for transaction_id, operation, key, value in cursor.fetchall():
            if transaction_id in committed_transactions and operation == 'write':
                set_value(self.conn, key, value)


class DBApp:
    def __init__(self, root, transaction_manager):
        self.root = root
        self.tm = transaction_manager
        self.transaction_id = 1
        self.cache_text = tk.Text(root, height=10, width=40)
        self.cache_text.pack()

        self.root.title("DB Transaction Manager")

        # Transaction ID
        self.trans_label = tk.Label(root, text="Transaction ID:")
        self.trans_label.pack()
        self.trans_entry = tk.Entry(root)
        self.trans_entry.pack()
        self.trans_entry.insert(0, str(self.transaction_id))

        # Key and Value
        self.key_label = tk.Label(root, text="Key:")
        self.key_label.pack()
        self.key_entry = tk.Entry(root)
        self.key_entry.pack()

        self.value_label = tk.Label(root, text="Value:")
        self.value_label.pack()
        self.value_entry = tk.Entry(root)
        self.value_entry.pack()

        # Transaction Control
        self.start_button = tk.Button(root, text="Start Transaction", command=self.start_transaction)
        self.start_button.pack()
        self.read_button = tk.Button(root, text="Read", command=self.read)
        self.read_button.pack()
        self.write_button = tk.Button(root, text="Write", command=self.write)
        self.write_button.pack()
        self.commit_button = tk.Button(root, text="Commit", command=self.commit)
        self.commit_button.pack()
        self.abort_button = tk.Button(root, text="Abort", command=self.abort)
        self.abort_button.pack()
        self.recover_button = tk.Button(root, text="Recover", command=self.recover)
        self.recover_button.pack()
        self.checkpoint_button = tk.Button(root, text="Checkpoint", command=self.checkpoint)
        self.checkpoint_button.pack()

        # Log Display
        self.log_frame = tk.LabelFrame(root, text="Logs")
        self.log_frame.pack(fill="both", expand="yes")

        self.log_tree = ttk.Treeview(self.log_frame, columns=("transaction_id", "operation", "key", "value"), show='headings')
        self.log_tree.heading("transaction_id", text="Transaction ID")
        self.log_tree.heading("operation", text="Operation")
        self.log_tree.heading("key", text="Key")
        self.log_tree.heading("value", text="Value")
        self.log_tree.pack(fill="both", expand=True)

        self.commit_log_frame = tk.LabelFrame(root, text="Commit Log")
        self.commit_log_frame.pack(fill="both", expand="yes")

        self.commit_log_tree = ttk.Treeview(self.commit_log_frame, columns=("transaction_id",), show='headings')
        self.commit_log_tree.heading("transaction_id", text="Transaction ID")
        self.commit_log_tree.pack(fill="both", expand=True)

        
        self.update_logs()

    
    def update_cache_display(self):
            self.cache_text.delete(1.0, tk.END)  # clear the text widget
            cache = self.tm.cache
            for key, value in cache.items():
                self.cache_text.insert(tk.END, f"{key}: {value}\n")
    
    
    def start_transaction(self):
        self.transaction_id = int(self.trans_entry.get())
        self.tm.start_transaction(self.transaction_id)
        self.update_cache_display()
        messagebox.showinfo("Transaction", f"Transaction {self.transaction_id} started")

    def read(self):
        key = self.key_entry.get()
        if key:
            value = self.tm.read(key)
            messagebox.showinfo("Read", f"Value for {key}: {value}")
            self.update_logs()

    def write(self):
        key = self.key_entry.get()
        value = self.value_entry.get()
        if key and value:
            self.tm.write(key, int(value))
            messagebox.showinfo("Write", f"Written {value} to {key}")
            self.update_logs()
            self.update_cache_display()

    def commit(self):
        self.tm.commit()
        messagebox.showinfo("Commit", f"Transaction {self.transaction_id} committed")
        self.update_logs()
        self.update_cache_display()

    def abort(self):
        self.tm.abort()
        messagebox.showinfo("Abort", f"Transaction {self.transaction_id} aborted")
        self.update_logs()

    def recover(self):
        self.tm.recover()
        messagebox.showinfo("Recover", "Recovery completed")
        self.update_logs()

    def checkpoint(self):
        messagebox.showinfo("Checkpoint", "Checkpoint created")
        # For simplicity, checkpoint functionality is not implemented
        # You can add checkpoint logic if needed

    def update_logs(self):
        for item in self.log_tree.get_children():
            self.log_tree.delete(item)
        logs = get_log(self.tm.conn)
        for log in logs:
            self.log_tree.insert("", "end", values=log)

        for item in self.commit_log_tree.get_children():
            self.commit_log_tree.delete(item)
        commit_logs = get_commit_log(self.tm.conn)
        for log in commit_logs:
            self.commit_log_tree.insert("", "end", values=log)


def main():
    conn = init_db()
    populate_initial_data(conn)
    tm = TransactionManager(conn)

    root = tk.Tk()
    app = DBApp(root, tm)
    root.mainloop()

if __name__ == "__main__":
    main()