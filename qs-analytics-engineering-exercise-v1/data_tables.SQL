DROP TABLE IF EXISTS Bars;
DROP TABLE IF EXISTS Glass;
DROP TABLE IF EXISTS Stock;
DROP TABLE IF EXISTS Drink;
DROP TABLE IF EXISTS Drink_Tags;
DROP TABLE IF EXISTS Drink_Instructions;
DROP TABLE IF EXISTS Drink_Recipe;
DROP TABLE IF EXISTS Transactions;

DROP TABLE IF EXISTS Drinks;

CREATE TABLE Bars (
    BarID int PRIMARY KEY NOT NULL,
    Name varchar NOT NULL,
    Curency Char(1));

INSERT INTO Bars values(1,'london','£'), (2,'budapest', '€'), (3, 'new york', '$');

CREATE TABLE Glass (
    GlassID int PRIMARY KEY NOT NULL,
    Name varchar NOT NULL );

CREATE TABLE Stock (
    RecordID integer PRIMARY KEY AUTOINCREMENT,
    CreatedDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FK_GlassID int Not NULL,
    FK_BarID int NOT NULL,
    stock int NOT NULL);

CREATE TABLE Drink (
    idDrink int PRIMARY KEY NOT NULL,
    drink varchar NOT NULL,
    Category varchar NOT NULL,
    Alcoholic boolean NOT NULL,
    Glass int NOT NULL);

CREATE TABLE Drink_Tags (
    FK_DrinkID int NOT NULL,
    Tags int NOT NULL);

CREATE TABLE Drink_Instructions (
    FK_DrinkID int NOT NULL,
    Language varchar,
    Instructions varchar NOT NULL);

CREATE TABLE Drink_Recipe (
    FK_DrinkID int NOT NULL,
    Step int,
    Ingredient varchar NOT NULL,
    Measurement varchar);

CREATE TABLE Transactions(
    TransactionID integer PRIMARY KEY AUTOINCREMENT,
    LogDateTime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FK_BarID int NOT NULL,
    TransactionTime TimeStamp,
    Item varchar,
    Amount double);
