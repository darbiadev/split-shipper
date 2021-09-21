use std::env;
use std::error::Error;
use std::fs::File;

use csv::Reader;
use rusqlite::{Connection, params, Result, Statement};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Shipment {
    shipment_id: String,
    company: String,
    attention_to: String,
    address1: String,
    address2: String,
    address3: String,
    city: String,
    state: String,
    postal_code: String,
    country: String,
}

fn load_shipments(shipments_file: &String) -> Result<Vec<Shipment>, Box<dyn Error>> {
    let mut reader: Reader<File> = csv::Reader::from_path(shipments_file)?;
    let mut shipments: Vec<Shipment> = Vec::new();

    for result in reader.deserialize() {
        let shipment: Shipment = result?;
        shipments.push(shipment)
    }

    Ok(shipments)
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let config_file: &String = &args[1];
    let shipments_file: &String = &args[2];

    let shipments: Vec<Shipment> = load_shipments(shipments_file).unwrap();

    let connection: Connection = Connection::open("test.db")?;
    connection.execute(
        "CREATE TABLE shipments (
                  shipment_id   TEXT PRIMARY KEY,
                  company       TEXT NOT NULL,
                  attention_to  TEXT NOT NULL,
                  address1      TEXT NOT NULL,
                  address2      TEXT NOT NULL,
                  address3      TEXT NOT NULL,
                  city          TEXT NOT NULL,
                  state         TEXT NOT NULL,
                  postal_code   TEXT NOT NULL,
                  country       TEXT NOT NULL
                  );",
        [],
    )?;

    let mut insert_statement: Statement = connection.prepare("INSERT INTO shipments (shipment_id, company, attention_to, address1, address2, address3, city, state, postal_code, country) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")?;
    for shipment in shipments {
        insert_statement.execute(&[&shipment.shipment_id, &shipment.company, &shipment.attention_to, &shipment.address1, &shipment.address2, &shipment.address3, &shipment.city, &shipment.state, &shipment.postal_code, &shipment.country])?;
    }

    let mut read_statement: Statement = connection.prepare("SELECT shipment_id, company, attention_to, address1, address2, address3, city, state, postal_code, country FROM shipments")?;
    let shipment_iter = read_statement.query_map([], |row| {
        Ok(Shipment {
            shipment_id: row.get(0)?,
            company: row.get(0)?,
            attention_to: row.get(0)?,
            address1: row.get(0)?,
            address2: row.get(0)?,
            address3: row.get(0)?,
            city: row.get(0)?,
            state: row.get(0)?,
            postal_code: row.get(0)?,
            country: row.get(0)?,
        })
    })?;

    for shipment in shipment_iter {
        println!("{:#?}", shipment.unwrap());
    }

    Ok(())
}
