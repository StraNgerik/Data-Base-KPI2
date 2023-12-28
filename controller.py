from model import Model, Train, Station, Transit, Ticket
from view import View
import time


class Controller:
    def __init__(self):
        self.model = Model()
        self.view = View()

    def run(self):
        while True:
            choice = self.show_tables()
            if choice == '1':
                choice = self.show_menu_train()
                if choice == '1':
                    self.add_train()
                elif choice == '2':
                    self.view_trains()
                elif choice == '3':
                    self.update_train()
                elif choice == '4':
                    self.delete_train()
                elif choice == '5':
                    self.generate_data_train()
                elif choice == '6':
                    break
            elif choice == '2':
                choice = self.show_menu_ticket()
                if choice == '1':
                    self.add_ticket()
                elif choice == '2':
                    self.view_tickets()
                elif choice == '3':
                    self.update_ticket()
                elif choice == '4':
                    self.delete_ticket()
                elif choice == '5':
                    self.generate_data_ticket()
                elif choice == '6':
                    break
            elif choice == '3':
                choice = self.show_menu_station()
                if choice == '1':
                    self.add_station()
                elif choice == '2':
                    self.view_stations()
                elif choice == '3':
                    self.update_station()
                elif choice == '4':
                    self.delete_station()
                elif choice == '5':
                    self.generate_data_station()
                elif choice == '6':
                    break
            elif choice == '4':
                choice = self.show_menu_transit()
                if choice == '1':
                    self.add_transit()
                elif choice == '2':
                    self.view_transits()
                elif choice == '3':
                    self.update_transit()
                elif choice == '4':
                    self.delete_transit()
                elif choice == '5':
                    self.generate_data_transit()
                elif choice == '6':
                    break

    def show_tables(self):
        self.view.show_message("\nChoose table:")
        self.view.show_message("1. Train")
        self.view.show_message("2. Ticket")
        self.view.show_message("3. Station")
        self.view.show_message("4. Transit")
        return input("Enter your choice: ")

    def show_menu_train(self):
        self.view.show_message("\nMenu:")
        self.view.show_message("1. Add Train")
        self.view.show_message("2. View Train")
        self.view.show_message("3. Update Train")
        self.view.show_message("4. Delete Train")
        self.view.show_message("5. Generate Data Train")
        self.view.show_message("6. Quit")
        return input("Enter your choice: ")

    def show_menu_ticket(self):
        self.view.show_message("\nMenu:")
        self.view.show_message("1. Add Ticket")
        self.view.show_message("2. View Tickets")
        self.view.show_message("3. Update Ticket")
        self.view.show_message("4. Delete Ticket")
        self.view.show_message("5. Generate random data ticket")
        self.view.show_message("6. Quit")
        return input("Enter your choice: ")

    def show_menu_transit(self):
        self.view.show_message("\nMenu:")
        self.view.show_message("1. Add Transit")
        self.view.show_message("2. View Transits")
        self.view.show_message("3. Update Transit")
        self.view.show_message("4. Delete Transit")
        self.view.show_message("5. Generate random data transit")
        self.view.show_message("6. Quit")
        return input("Enter your choice: ")

    def show_menu_station(self):
        self.view.show_message("\nMenu:")
        self.view.show_message("1. Add Station")
        self.view.show_message("2. View Station")
        self.view.show_message("3. Update Station")
        self.view.show_message("4. Delete Station")
        self.view.show_message("5. Generate random data station")
        self.view.show_message("6. Quit")
        return input("Enter your choice: ")

    def add_train(self):
        stsamnt = self.view.get_train_input()
        if self.model.add_train(stsamnt):
            self.view.show_message("Error, Impossible value!")
        else:
            self.view.show_message("Train added successfully!")



    def view_trains(self):
        start_time = time.time()
        trains = self.model.get_train_attr_table()
        #trains = self.model.get_train_by_num()
        end_time = time.time()
        self.view.show_trains(trains)
        self.view.show_exec_time(end_time - start_time)

    def update_train(self):
        train_num = self.view.get_id()
        stsamnt = self.view.get_train_input()
        err_code = self.model.update_train(train_num, stsamnt)
        if err_code == 2:
            self.view.show_message("Error, Impossible value!")
        if err_code == 1:
            self.view.show_message("Error, Train with such number doesn't exist!")
        if err_code == 0:
            self.view.show_message("Train updated successfully!")

    def delete_train(self):
        train_num = self.view.get_id()
        if self.model.delete_train(train_num):
            self.view.show_message("Error, Train with such number doesn't exist!")
        else:
            self.view.show_message("Train deleted successfully!")

    def generate_data_train(self):
        count = self.view.get_count()
        self.model.generate_data_train(count)

    def add_ticket(self):
        prc, trs_id, pas_fn = self.view.get_ticket_input()
        if self.model.add_ticket(prc, trs_id, pas_fn):
            self.view.show_message("Error, Transit id doesn't exist!")
        else:
            self.view.show_message("Ticket added successfully!")

    def view_tickets(self):
        start_time = time.time()
        tickets = self.model.get_ticket_attr_table()
        #tickets = self.model.get_ticket_by_num()
        end_time = time.time()
        self.view.show_tickets(tickets)
        self.view.show_exec_time(end_time - start_time)

    def update_ticket(self):
        idd = self.view.get_id()
        prc, trs_id, pas_fn = self.view.get_ticket_input()
        if self.model.update_ticket(idd, prc, trs_id, pas_fn):
            self.view.show_message("Error, Ticket ID or Transit ID incorrect")
        else:
            self.view.show_message("Ticket updated successfully!")

    def delete_ticket(self):
        idd = self.view.get_id()
        if self.model.delete_ticket(idd):
            self.view.show_message("Error, no such ticket!")
        else:
            self.view.show_message("Ticket deleted successfully!")

    def generate_data_ticket(self):
        count = self.view.get_count()
        self.model.generate_data_ticket(count)

    def add_station(self):
        name = self.view.get_station_input()
        self.model.add_station(name)
        self.view.show_message("Station added successfully!")

    def view_stations(self):
        start_time = time.time()
        #stations = self.model.get_station_attr_table()
        stations = self.model.get_station_by_num()
        end_time = time.time()
        self.view.show_stations(stations)
        self.view.show_exec_time(end_time - start_time)

    def update_station(self):
        st_id = self.view.get_id()
        name = self.view.get_station_input()
        if self.model.update_station(st_id, name):
            self.view.show_message("Error, no such station!")
        else:
            self.view.show_message("Station updated successfully!")

    def delete_station(self):
        st_id = self.view.get_id()
        if self.model.delete_station(st_id):
            self.view.show_message("Error, no such station!")
        else:
            self.view.show_message("Station deleted successfully!")

    def generate_data_station(self):
        count = self.view.get_count()
        self.model.generate_data_station(count)

    def add_transit(self):
        tr_num, st_id, date = self.view.get_transit_input()
        if self.model.add_transit(tr_num, st_id, date):
            self.view.show_message("Error, one of id doesn't exist!")
        else:
            self.view.show_message("Transit added successfully!")

    def view_transits(self):
        transits = self.model.get_transit_attr_table()
        self.view.show_transits(transits)

    def update_transit(self):
        trs_id = self.view.get_id()
        tr_num, st_id, date = self.view.get_transit_input()
        if self.model.update_transit(tr_num, st_id, date, trs_id):
            self.view.show_message("Error, Transit id or Train num or Station ID incorrect!")
        else:
            self.view.show_message("Transit updated successfully!")

    def delete_transit(self):
        trs_id = self.view.get_id()
        if self.model.delete_transit(trs_id):
            self.view.show_message("Error no such Transit id!")
        else:
            self.view.show_message("Transit deleted successfully!")

    def generate_data_transit(self):
        count = self.view.get_count()
        self.model.generate_data_transit(count)
