from tkinter import *
from tkinter import filedialog, messagebox
import main
import os


def run_gui():
    def init_clear_data():
        def clear_data_callback():
            fn = clear_data_file_name.get()
            if fn:
                cl_all = clear_all.get()
                main.remove_file_from_cluster(os.path.basename(fn), cl_all)
                messagebox.showinfo("Done", "Done")
            else:
                messagebox.showerror("Error", "Please specify file name!")

        clear_data_label = Label(main_frame, text="Please enter file name:")
        clear_data_label.pack(pady=20)
        file_name = StringVar(main_frame, value="spotify_data.csv")
        clear_data_file_name = Entry(main_frame, textvariable=file_name)
        clear_data_file_name.pack(pady=5)
        clear_all = IntVar(main_frame)
        clear_all.set(1)
        clear_all_checkbutton = Checkbutton(main_frame, text="Clear all?", variable=clear_all)
        clear_all_checkbutton.pack(pady=20)
        submit = Button(main_frame, text="Go", command=clear_data_callback)
        submit.pack(pady=20)

    def init_push_file_on_cluster():
        def pfc_run():
            fn = src_file_name.get()
            main.push_file_on_cluster(fn, os.path.basename(fn))
            messagebox.showinfo("Done", "Done")

        def choose_src_file_callback():
            fn = filedialog.askopenfile()
            if fn:
                src_file_name.set(fn.name)
                src_file_name_label = Label(main_frame, text=src_file_name.get())
                src_file_name_label.pack(pady=20)
                dest_file_name = StringVar(main_frame, os.path.basename(src_file_name.get()))
                dest_file_name_label = Label(main_frame, text="Dest file name:")
                dest_file_name_entry = Entry(main_frame, textvariable=dest_file_name)
                dest_file_name_label.pack(pady=5)
                dest_file_name_entry.pack(pady=5)
                pfc_btn = Button(main_frame, text="Push file on cluster", command=pfc_run)
                pfc_btn.pack(pady=20)

        src_file_name = StringVar(main_frame)
        src_file_btn = Button(main_frame, command=choose_src_file_callback, text="Choose source file")
        src_file_btn.pack(pady=20)

    def init_run_map_reduce():
        def run_map_reduce():
            sql_query = sql_entry.get("1.0", END)
            if not sql_query.replace("\n", "").replace(" ", "").replace("\t", ""):
                messagebox.showerror("Error", "Please set SQL query!")
                return
            main.run_tasks(sql_query)
            messagebox.showinfo("Done", "Done")

        mr_label = Label(main_frame, text="Please enter SQL query:")
        mr_label.pack(ipady=10)
        default_sql_command = "SELECT B.Streams, A.Artist as musician, A.URL FROM A.csv INNER JOIN B.csv ON A.URL=B.URL;"
        # default_sql_command = "SELECT Artist, SUM(Streams) FROM spotify_data.csv WHERE Streams > 10000 AND Region NOT IN ('ua, us') OR Position BETWEEN 20 AND 100 GROUP BY 'Artist';"
        # default_sql_command = "SELECT A.URL, A.Position FROM A.csv JOIN B.csv ON A.URL = B.URL;"
        # default_sql_command = "SELECT * FROM spotify_data.csv WHERE Artist LIKE 'C%';"

        # default_sql_command = "SELECT * FROM B.csv WHERE Position = 8;"
        # default_sql_command = "SELECT Artist, SUM(Streams) FROM spotify_data.csv GROUP BY 'Artist';"
        # clr data
        # default_sql_command = "SELECT * FROM spotify_data.csv;"
        # default_sql_command += "SELECT * FROM spotify_data.csv WHERE Artist LIKE 'C%' AND Streams BETWEEN 1000 AND 2000;"
        # default_sql_command = "SELECT * FROM (SELECT Streams, Position FROM spotify_data.csv) WHERE Streams < 10000;"
        # default_sql_command = "SELECT * FROM (SELECT Position, Artist, Streams FROM spotify_data.csv) WHERE Position IN (1, 2, 3) OR Streams BETWEEN 1000 AND 5000;"
        # default_sql_command = "SELECT Position, Artist FROM " \
        #                       "(SELECT * FROM " \
        #                       "(SELECT Position, Artist, Streams FROM spotify_data.csv) " \
        #                       "WHERE Position IN (1, 2, 3) OR Streams BETWEEN 1000 AND 5000) " \
        #                       "WHERE Artist LIKE '% % %';"
        # default_sql_command = "SELECT Artist, Position FROM spotify_data.csv WHERE Region IN ('ua', 'us') OR Position NOT LIKE '___';"
        default_sql_command = "SELECT * FROM (SELECT B.Streams, A.Artist as Musician, A.URL FROM A.csv INNER JOIN B.csv ON A.URL=B.URL) WHERE Musician LIKE '% %';"
        sql_entry = Text(main_frame)
        sql_entry.pack(fill=BOTH, expand=1, padx=20, pady=10)
        sql_entry.insert(END, default_sql_command)
        submit_btn = Button(main_frame, text="Run map reduce", command=run_map_reduce)
        submit_btn.pack(pady=20)

    def init_get_file():
        def run_get_file():
            fn = get_file_name.get()
            if fn:
                print("GETTING FILE!")
                print(fn)
                messagebox.showinfo("Done", "Done")
            else:
                messagebox.showerror("Error", "Please specify file name!")

        get_file_label = Label(main_frame, text="Please specify file name:")
        get_file_label.pack(pady=20)
        get_file_name = StringVar(main_frame)
        get_file_entry = Entry(main_frame, textvariable=get_file_name)
        get_file_entry.pack(pady=5)
        get_file_button = Button(main_frame, text="Get file", command=run_get_file)
        get_file_button.pack(pady=20)

    def select_rb():
        choice = rb_var.get()
        for widget in main_frame.winfo_children():
            widget.destroy()
        if choice == "1":
            init_clear_data()
        elif choice == "2":
            init_push_file_on_cluster()
        elif choice == "3":
            init_run_map_reduce()
        elif choice == "4":
            init_get_file()

    root = Tk()
    root.wm_attributes("-topmost", 1)
    width = 800
    height = 600
    master_name = "Map Reduce"
    root.title(master_name)

    ws = root.winfo_screenwidth()
    hs = root.winfo_screenheight()
    x_pad = ws // 2 - width // 2
    y_pad = hs // 2 - height // 2
    root.geometry(f"{width}x{height}+{x_pad}+{y_pad}")
    # root.resizable(width=False, height=False)
    root.minsize(width=width, height=height)

    rb_var = StringVar(root, "3")

    options = {
        "clear data": "1",
        "push file on cluster": "2",
        "run map reduce": 3,
        "get file": 4
    }
    col = 0
    rb_frame = Frame(root)
    rb_frame.pack(fill=X)
    choose_label = Label(rb_frame, text="Please choose the option:")
    choose_label.pack(ipady=10)
    for key, value in options.items():
        rb = Radiobutton(rb_frame, text=key, value=value, variable=rb_var, command=select_rb)
        rb.pack(side=LEFT, fill=X, expand=1)
        col += 1
    main_frame = Frame(root)
    main_frame.pack(fill=BOTH, expand=1)
    select_rb()

    root.mainloop()


if __name__ == "__main__":
    run_gui()
