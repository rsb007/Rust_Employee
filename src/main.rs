#[macro_use]
extern crate cdrs;
#[macro_use]
extern crate cdrs_helpers_derive;
#[macro_use]
extern crate maplit;

use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::frame::IntoBytes;
use cdrs::load_balancing::RoundRobin;
use cdrs::query::*;
use cdrs::types::from_cdrs::FromCDRSByName;
use cdrs::types::prelude::*;
use std::collections::HashMap;

type CurrentSession = Session<RoundRobin<TcpConnectionPool<NoneAuthenticator>>>;


fn main() {
    let node = NodeTcpConfigBuilder::new("127.0.0.1:9042", NoneAuthenticator {}).build();
    let cluster_config = ClusterTcpConfig(vec![node]);
    let no_compression: CurrentSession =
        new_session(&cluster_config, RoundRobin::new()).expect("session should be created");

    create_keyspace(&no_compression);
    create_table(&no_compression);
    insert_struct(&no_compression);
    select_struct(&no_compression);
    update_struct(&no_compression);
    delete_struct(&no_compression);
}


#[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq)]
struct Employee {
    emp_id: String,
    emp_name: String,
    emp_salary: f32,
    emp_mobile: String,
}


fn create_keyspace(session: &CurrentSession) {
    let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS employee WITH REPLICATION = { \
                                 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
    session.query(create_ks).expect("Keyspace creation error");
}

fn create_table(session: &CurrentSession) {
    let create_table_cql =
        "CREATE TABLE employee.emp_details (
    emp_id text,
    emp_name text,
    emp_salary float,
    emp_mobile text,
    PRIMARY KEY (emp_id))";
    session
        .query(create_table_cql)
        .expect("Table creation error");
}


fn insert_struct(session: &CurrentSession) {
    let row: Employee = Employee {
        emp_id: "John cena".to_string(),
        emp_name: "John Doe".to_string(),
        emp_salary: 100000.00,
        emp_mobile: "123456789".to_string(),
    };

    let insert_struct_cql = "INSERT INTO employee.emp_details \
                           (emp_id, emp_name, emp_salary, emp_mobile) VALUES (?, ?, ?, ?)";
    session
        .query_with_values(insert_struct_cql, row.into_query_values())
        .expect("insert error ");
}

impl Employee {
    fn into_query_values(self) -> QueryValues {
        query_values!(self.emp_id, self.emp_name, self.emp_salary, self.emp_mobile)
    }
}

fn select_struct(session: &CurrentSession)
{
    let select_struct_cql = "Select * from employee.emp_details";
    let rows = session
        .query(select_struct_cql)
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into row");

    for row in rows {
        let my_row: Employee = Employee::try_from_row(row).expect("into Employee Struct");
        println!("Struct got :{:?}", my_row);
    }
}


fn delete_struct(session: &CurrentSession)
{
    let delete_struct_cql = "Delete from employee.emp_details where emp_id = ?";
    let user_key = "John cena";
    session
        .query_with_values(delete_struct_cql, query_values!(user_key))
        .expect("delete");
}

fn update_struct(session: &CurrentSession) {
    let update_struct_cql = "Update employee.emp_details SET emp_name = ? where emp_id = ?";
    let emp_name = "John Cena";
    let emp_id = "John";
    session
        .query_with_values(update_struct_cql, query_values!(emp_name, emp_id))
        .expect("update takes place");
}


