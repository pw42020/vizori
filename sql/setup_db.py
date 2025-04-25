import os
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    age = Column(Integer)
    email = Column(String, unique=True)

    orders = relationship("Order", back_populates="user")


class Food(Base):
    __tablename__ = "foods"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    price = Column(Float)

    orders = relationship("Order", back_populates="food")


class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    food_id = Column(Integer, ForeignKey("foods.id"))

    user = relationship("User", back_populates="orders")
    food = relationship("Food", back_populates="orders")


DATABASE_URL = "sqlite:///test.db"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    if not os.path.exists("test.db"):
        Base.metadata.create_all(bind=engine)
        print("Database initialized.")
    else:
        print("Database already exists.")
        return

    session = SessionLocal()
    users = [
        User(name="Alice", age=30, email="alice@gmail.com"),
        User(name="Bob", age=25, email="bob@gmail.com"),
        User(name="Charlie", age=35, email="charlie@gmail.com"),
    ]
    session.add_all(users)
    session.commit()

    foods = [
        Food(name="Pizza", price=9.99),
        Food(name="Burger", price=5.99),
        Food(name="Salad", price=4.99),
    ]

    session.add_all(foods)
    session.commit()

    orders = [
        Order(user_id=1, food_id=1),
        Order(user_id=1, food_id=2),
        Order(user_id=2, food_id=3),
    ]
    session.add_all(orders)
    session.commit()
    session.close()


if __name__ == "__main__":
    init_db()
