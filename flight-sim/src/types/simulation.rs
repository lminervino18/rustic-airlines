use std::collections::HashMap;
use std::io::Write;
use std::sync::{mpsc, Arc, Mutex, RwLock, RwLockReadGuard};
use std::time::Duration;
use std::{io, thread};
use threadpool::ThreadPool;

use super::airport::Airport;
use super::client::Client;
use super::flight::Flight;

use super::flight_status::FlightStatus;
use super::sim_error::SimError;
use super::timer::Timer;
use super::TICK_FREQUENCY_MILLIS;

/// Manages the overall state of the flight simulation.
///
/// The `Simulation` struct contains flights, airports, a timer, and a thread pool for executing
/// periodic tasks like updating flight data and syncing with a database.
pub struct Simulation {
    pub flights: Arc<RwLock<HashMap<String, Arc<RwLock<Flight>>>>>, // Flights wrapped in Arc<RwLock>
    pub airports: Arc<RwLock<HashMap<String, Airport>>>,            // Airports
    pub db: Arc<Mutex<Client>>,                                     // DB Client protected by Mutex
    pub timer: Arc<Timer>,                                          // Timer
    pub thread_pool: Arc<ThreadPool>,                               // ThreadPool
}

impl Simulation {
    /// Create a new simulation
    pub fn new(db: Arc<Mutex<Client>>, timer: Arc<Timer>, thread_pool: Arc<ThreadPool>) -> Self {
        Simulation {
            flights: Arc::new(RwLock::new(HashMap::new())),
            airports: Arc::new(RwLock::new(HashMap::new())),
            db,
            timer,
            thread_pool,
        }
    }

    /// Start the simulation
    pub fn start(&self) {
        let flights = Arc::clone(&self.flights);
        let airports = Arc::clone(&self.airports);
        let db = Arc::clone(&self.db);
        let thread_pool = Arc::clone(&self.thread_pool);
        let timer = Arc::clone(&self.timer);

        let _ = timer.start(move |current_time, tick_count| {
            {
                if let Ok(flights_lock) = flights.try_read() {
                    for flight_arc in flights_lock.values() {
                        let flight = Arc::clone(flight_arc);
                        let db = Arc::clone(&db);

                        if !should_update(&flight) {
                            continue;
                        }

                        thread_pool.execute(move || {
                            if let Ok(mut flight_lock) = flight.try_write() {
                                let updated_state =
                                    flight_lock.check_states_and_update_flight(current_time);

                                // Update the database
                                if let Ok(mut db_lock) = db.lock() {
                                    let result = if updated_state {
                                        db_lock.update_flight_status(&flight_lock)
                                    } else {
                                        db_lock.update_flight(&flight_lock)
                                    };

                                    if let Err(e) = result {
                                        eprintln!("Database update error: {:?}", e);
                                    }
                                } else {
                                    eprintln!("Failed to lock DB for updating flight.");
                                }
                            } else {
                                eprintln!("Failed to lock flight for update. Skipping.");
                            }
                        });
                    }
                } else {
                    eprintln!("Failed to read flights. Skipping this cycle.");
                }
            }

            // Synchronize with the database every 5 ticks
            if tick_count % 5 == 0 {
                let mut flights_from_db = Vec::new();
                {
                    if let Ok(mut db_lock) = db.lock() {
                        if let Ok(airport_list) = airports.read() {
                            flights_from_db =
                                match db_lock.fetch_flights(current_time, &airport_list) {
                                    Ok(flights) => flights,
                                    Err(e) => {
                                        eprintln!("Failed to fetch flights from DB: {:?}", e);
                                        return;
                                    }
                                };
                        } else {
                            eprintln!("Failed to lock airports for read. Skipping database sync.");
                        }
                    } else {
                        eprintln!("Failed to lock DB for fetching flights. Skipping.");
                        return;
                    }
                }

                if let Ok(mut flights_lock) = flights.try_write() {
                    for mut flight in flights_from_db {
                        match flights_lock.get(&flight.flight_number) {
                            Some(existing_flight) => {
                                if let Ok(mut flight_lock) = existing_flight.write() {
                                    if flight_lock.status != flight.status {
                                        flight_lock.status = flight.status;
                                        flight_lock.check_states_and_update_flight(current_time);
                                        if let Ok(mut db_lock) = db.lock() {
                                            let result = db_lock.update_flight_status(&flight_lock);
                                            if let Err(e) = result {
                                                eprintln!("Database update error: {:?}", e);
                                            }
                                        } else {
                                            eprintln!(
                                                "Failed to lock DB for fetching flights. Skipping."
                                            );
                                            return;
                                        }
                                    }
                                } else {
                                    eprintln!(
                                        "Failed to lock flight {} for update. Skipping.",
                                        flight.flight_number
                                    );
                                }
                            }
                            None => {
                                if let Ok(mut db_lock) = db.lock() {
                                    if let Err(e) = db_lock
                                        .fetch_flight_info(&mut flight, &airports.read().unwrap())
                                    {
                                        eprintln!(
                                            "Failed to fetch additional flight info for {:?}: {:?}",
                                            flight.flight_number, e
                                        );
                                        continue;
                                    }
                                }
                                flights_lock.insert(
                                    flight.flight_number.clone(),
                                    Arc::new(RwLock::new(flight)),
                                );
                            }
                        }
                    }
                } else {
                    eprintln!("Failed to lock flights for writing. Skipping database sync.");
                }
            }
        });
    }

    /// Adds an airport to the simulation.
    pub fn add_airport(&self, airport: Airport) -> Result<(), SimError> {
        {
            let mut db = self.db.lock().map_err(|_| SimError::ClientError)?;
            db.insert_airport(&airport)
                .map_err(|_| SimError::ClientError)?;
        }

        let mut airports_lock = self
            .airports
            .write()
            .map_err(|_| SimError::Other("Failed to lock airports".to_string()))?;
        airports_lock.insert(airport.iata_code.clone(), airport);

        Ok(())
    }

    /// Adds a flight to the simulation.
    pub fn add_flight(&self, flight: Flight) -> Result<(), SimError> {
        {
            let mut db = self.db.lock().map_err(|_| SimError::ClientError)?;
            db.insert_flight(&flight)
                .map_err(|_| SimError::ClientError)?;
        }

        let mut flights_lock = self
            .flights
            .write()
            .map_err(|_| SimError::Other("Failed to lock flights".to_string()))?;
        flights_lock.insert(flight.flight_number.clone(), Arc::new(RwLock::new(flight)));

        Ok(())
    }

    /// Displays the flights in real time
    pub fn display_flights(&self) {
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let mut buffer = String::new();
            loop {
                buffer.clear();
                if io::stdin().read_line(&mut buffer).is_ok() && !buffer.trim().is_empty() {
                    tx.send(()).ok();
                    break;
                }
                thread::sleep(Duration::from_millis(100));
            }
        });

        loop {
            io::stdout().flush().ok();

            if let Ok(flights_lock) = self.flights.try_read() {
                print!("\x1B[2J\x1B[1;1H");
                if let Ok(time) = self.timer.current_time.try_lock() {
                    println!("Current time: {}", time.format("%d-%m-%Y %H:%M:%S"));
                }
                if flights_lock.is_empty() {
                    println!("No flights available.");
                } else {
                    println!(
                        "\n{:<15} {:<10} {:<15} {:<15} {:<10} {:<10}",
                        "Flight Number", "Status", "Origin", "Destination", "Latitude", "Longitude"
                    );
                    for flight_arc in flights_lock.values() {
                        if let Ok(flight_lock) = flight_arc.try_read() {
                            println!(
                                "{:<15} {:<10} {:<15} {:<15} {:<10.4} {:<10.4}",
                                flight_lock.flight_number,
                                flight_lock.status.as_str(),
                                flight_lock.origin.iata_code,
                                flight_lock.destination.iata_code,
                                flight_lock.latitude,
                                flight_lock.longitude
                            );
                        }
                    }
                }
                println!("\nPress 'q' and Enter to exit list-flights mode");
            }

            if rx.try_recv().is_ok() {
                break;
            }

            thread::sleep(Duration::from_millis(TICK_FREQUENCY_MILLIS));
        }
    }

    /// List the airports in the simulation
    pub fn list_airports(&self) {
        if let Ok(airports_lock) = self.airports.read() {
            if airports_lock.is_empty() {
                println!("No airports available.");
            } else {
                println!("\n{:<10} {:<30}", "IATA Code", "Airport Name");
                for airport in airports_lock.values() {
                    println!("{:<10} {:<30}", airport.iata_code, airport.name);
                }
            }
        } else {
            eprintln!("Failed to read airports.");
        }
    }

    pub fn set_time_rate(&self, minutes: i64) -> Result<(), SimError> {
        self.timer.set_tick_advance(minutes)
    }

    /// Stop the timer and the threadpool.
    pub fn stop(&self) {
        self.timer.stop();
        self.thread_pool.join();
    }

    /// Return a clone of the list of airports
    pub fn get_airports(&self) -> Result<RwLockReadGuard<HashMap<String, Airport>>, SimError> {
        self.airports
            .read()
            .map_err(|_| SimError::AirportNotFound("Could not read airports".to_string()))
    }

    pub fn pause_simulation(&mut self) {
        self.timer.pause();
    }

    pub fn resume_simulation(&mut self) {
        self.timer.resume();
    }
}

fn should_update(flight: &Arc<RwLock<Flight>>) -> bool {
    if let Ok(flight_lock) = flight.try_read() {
        if flight_lock.status != FlightStatus::Canceled
            && flight_lock.status != FlightStatus::Finished
        {
            return true;
        }
    }
    false
}
