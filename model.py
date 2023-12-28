from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey, text, desc, asc, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.engine.reflection import Inspector
import random
from datetime import datetime, timedelta

Base = declarative_base()


class Train(Base):
    __tablename__ = 'Train'

    Number = Column(Integer, primary_key=True)
    Seats_amount = Column(Integer, nullable=False)


class Station(Base):
    __tablename__ = 'Station'

    ID = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)


class Transit(Base):
    __tablename__ = 'Transit'

    Train_num = Column(Integer, ForeignKey('Train.Number'), nullable=False)
    Station_id = Column(Integer, ForeignKey('Station.ID'), nullable=False)
    Tab_id = Column(Integer, primary_key=True)
    Date = Column(Date, nullable=False)

    train = relationship('Train')
    station = relationship('Station')


class Ticket(Base):
    __tablename__ = 'Ticket'

    ID = Column(Integer, primary_key=True)
    Price = Column(Integer, nullable=False)
    Transit_id = Column(Integer, ForeignKey('Transit.Tab_id'), nullable=False)
    Pas_full_name = Column(String(50), nullable=False)

    transit = relationship('Transit')


class Model:
    def __init__(self):
        engine = create_engine('postgresql://postgres:q123w456@localhost:8080/KiriloZaliznitsya', echo=True)
        session = sessionmaker(bind=engine)
        self.session = session()
        inspector = Inspector.from_engine(engine)
        indexes = inspector.get_indexes('Train')
        index_exists = any(index['column_names'] == ['Seats_amount'] for index in indexes)
        if not index_exists:
            index1 = Index('Train_BRIN_Seat', Train.Seats_amount, postgresql_using='BRIN')
            index1.create(engine)
        indexes = inspector.get_indexes('Ticket')
        index_exists = any(index['column_names'] == ['Price'] for index in indexes)
        if not index_exists:
            index2 = Index('Ticket_price_hash',Ticket.Price, postgresql_using='hash')
            index2.create(engine)

        tripper = '''
        DROP TRIGGER trigger_check_and_insert_rating ON "Train";
        
        DROP FUNCTION IF EXISTS check_and_insert_rating();
        
        CREATE OR REPLACE FUNCTION check_and_insert_rating()
        RETURNS TRIGGER 
        LANGUAGE plpgsql
        AS $$
        BEGIN
        IF NEW."Seats_amount" < 0 OR NEW."Seats_amount" > 5 THEN
        RAISE EXCEPTION 'Rating must be between 0 and 5';
        ELSE
        RETURN NEW;
        END IF;
        END;
        $$;
        
        CREATE TRIGGER trigger_check_and_insert_rating
        AFTER INSERT OR UPDATE
        ON "Train"
        FOR EACH ROW
        EXECUTE FUNCTION check_and_insert_rating();
        '''
        self.session.execute(text(tripper))
        self.session.commit()

    def get_train_attr_table(self):
        output = [(train.Number, train.Seats_amount) for train in self.session.query(Train).all()]
        return output

    def get_train_by_num(self):
        train = self.session.query(Train).filter_by(Seats_amount=1031).first()
        output = [(train.Number, train.Seats_amount)]
        return output

    def get_station_attr_table(self):
        output = [(station.ID, station.name) for station in self.session.query(Station).all()]
        return output

    def get_station_by_num(self):
        station = self.session.query(Station).filter_by(name='Kiyv').first()
        output = [(station.ID, station.name)]
        return output

    def get_transit_attr_table(self):
        output = [(transit.Train_num, transit.Station_id,
                   transit.Tab_id, transit.Date) for transit in self.session.query(Transit).all()]
        return output

    def get_ticket_attr_table(self):
        output = [(ticket.ID, ticket.Price,
                   ticket.Transit_id, ticket.Pas_full_name) for ticket in self.session.query(Ticket).all()]
        return output

    def get_ticket_by_num(self):
        ticket = self.session.query(Ticket).filter_by(Price=1613).first()
        output = [(ticket.ID, ticket.Price, ticket.Transit_id, ticket.Pas_full_name)]
        return output
    def add_train(self, stsam):
        try:
            train = Train(Seats_amount=stsam)
            self.session.add(train)
            self.session.commit()
            return 0
        except:
            self.session.rollback()
            return 1

    def update_train(self, num, stsam):
        train = self.session.query(Train).filter_by(Number=num).first()
        if train:
            train.Seats_amount = stsam
            try:
                self.session.commit()
            except:
                self.session.rollback()
                return 2
            return 0
        else:
            return 1

    def delete_train(self, num):
        train = self.session.query(Train).filter_by(Number=num).first()
        if train:
            self.session.delete(train)
            self.session.commit()
            return 0
        else:
            return 1

    def add_ticket(self, prc, trs_id, pas_nm):
        transit = self.session.query(Transit).filter_by(Tab_id=trs_id).first()
        if transit:
            ticket = Ticket(Price=prc, Transit_id=trs_id, Pas_full_name=pas_nm)
            self.session.add(ticket)
            self.session.commit()
            return 0
        else:
            return 1

    def update_ticket(self, idd, prc, trs_id, pas_nm):
        ticket = self.session.query(Ticket).filter_by(ID=idd).first()
        transit = self.session.query(Transit).filter_by(Tab_id=trs_id).first()
        if ticket and transit:
            ticket.Price = prc
            ticket.Transit_id = trs_id
            ticket.Pas_full_name = pas_nm
            self.session.commit()
            return 0
        else:
            return 1

    def delete_ticket(self, idd):
        ticket = self.session.query(Ticket).filter_by(ID=idd).first()
        if ticket:
            self.session.delete(ticket)
            self.session.commit()
            return 0
        return 1

    def add_station(self, name):
        station = Station(name=name)
        self.session.add(station)
        self.session.commit()

    def update_station(self, idd, name):
        station = self.session.query(Station).filter_by(ID=idd).first()
        if station:
            station.name = name
            self.session.commit()
            return 0
        else:
            return 1

    def delete_station(self, idd):
        station = self.session.query(Station).filter_by(ID=idd).first()
        if station:
            self.session.delete(station)
            self.session.commit()
            return 0
        else:
            return 1

    def create_table_transit(self):
        pass  # Already created using declarative_base()

    def add_transit(self, tr_num, st_id, date):
        train = self.session.query(Train).filter_by(Number=tr_num).first()
        station = self.session.query(Station).filter_by(ID=st_id).first()
        if train and station:
            transit = Transit(Train_num=tr_num, Station_id=st_id, Date=date)
            self.session.add(transit)
            self.session.commit()
            return 0
        else:
            return 1

    def update_transit(self, tr_num, st_id, date, trs_id):
        transit = self.session.query(Transit).filter_by(Tab_id=trs_id).first()
        train = self.session.query(Train).filter_by(Number=tr_num).first()
        station = self.session.query(Station).filter_by(ID=st_id).first()
        if transit and train and station:
            transit.Train_num = tr_num
            transit.Station_id = st_id
            transit.Date = date
            self.session.commit()
            return 0
        else:
            return 1

    def delete_transit(self, trs_id):
        transit = self.session.query(Transit).filter_by(Tab_id=trs_id).first()
        if transit:
            self.session.delete(transit)
            self.session.commit()
            return 0
        else:
            return 1

    def generate_data_train(self, count):
        for _ in range(count):
            self.add_train(random.randint(1000, 2000))

    def generate_data_station(self, count):
        names = ['Kiyv', 'Chernihiv', 'Nizhyn', 'Novoselivka', 'Sumy', 'Studenyky']
        for _ in range(count):
            self.add_station(random.choice(names))

    def generate_data_transit(self, count):
        last_id_tr = self.session.query(Train.Number).order_by(desc(Train.Number)).first()
        first_id_tr = self.session.query(Train.Number).order_by(asc(Train.Number)).first()
        last_id_st = self.session.query(Station.ID).order_by(desc(Station.ID)).first()
        first_id_st = self.session.query(Station.ID).order_by(asc(Station.ID)).first()
        for _ in range(count):
            tr_num = random.randint(int(first_id_tr[0]), int(last_id_tr[0]))
            st_id = random.randint(int(first_id_st[0]), int(last_id_st[0]))
            date = datetime.now() - timedelta(days=random.randint(1, 365))
            self.add_transit(tr_num, st_id, date)

    def generate_data_ticket(self, count):
        last_id_tr = self.session.query(Transit.Tab_id).order_by(desc(Transit.Tab_id)).first()
        first_id_tr = self.session.query(Transit.Tab_id).order_by(asc(Transit.Tab_id)).first()
        for _ in range(count):
            prc = random.randint(1000, 2000)
            trs_id = random.randint(int(first_id_tr[0]), int(last_id_tr[0]))
            pas_nm = random.choice(['Alice', 'Bob', 'Charlie', 'David', 'Eva', 'Frank'])
            self.add_ticket(prc, trs_id, pas_nm)
