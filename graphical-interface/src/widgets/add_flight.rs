use crate::{
    db::{Db, Provider},
    types::{Airport, Flight, FlightInfo, FlightStatus},
};
use chrono::{NaiveDate, Utc};
use egui::{self};
use egui_extras::DatePickerButton;
use walkers::Position;

/// A widget for adding a new flight through a graphical interface.
///
/// This widget collects flight information such as flight number, departure and arrival details,
/// fuel, height, speed, origin, and destination. It validates the input and submits it to the database.
pub struct WidgetAddFlight {
    is_open: bool,
    flight_number: String,
    departure_date: NaiveDate,
    departure_hour: u32,
    departure_minute: u32,
    arrival_date: NaiveDate,
    arrival_hour: u32,
    arrival_minute: u32,
    fuel: f64,
    height: i32,
    speed: i32,
    origin: String,
    destination: String,
    error_message: Option<String>, // Para mostrar errores al usuario
}

impl WidgetAddFlight {
    /// Creates a new `WidgetAddFlight` instance with default values.
    pub fn new() -> Self {
        Self {
            is_open: true,
            flight_number: String::new(),
            departure_date: Utc::now().date_naive(),
            departure_hour: 0,
            departure_minute: 0,
            arrival_date: Utc::now().date_naive(),
            arrival_hour: 0,
            arrival_minute: 0,
            fuel: 0.0,
            height: 0,
            speed: 0,
            origin: String::new(),
            destination: String::new(),
            error_message: None,
        }
    }

    /// Widget interface for adding new flights.
    pub fn show(&mut self, ctx: &egui::Context, db: &mut Db, airports: &[Airport]) -> bool {
        let mut is_open: bool = self.is_open;
        let mut should_close: bool = false;

        egui::Window::new("Add Flight")
            .open(&mut is_open)
            .show(ctx, |ui| {
                ui.vertical(|ui| {
                    ui.label("Fill in the details of the new flight:");

                    ui.horizontal(|ui| {
                        ui.label("Flight Number:");
                        ui.text_edit_singleline(&mut self.flight_number);
                    });

                    ui.horizontal(|ui| {
                        ui.label("Departure:");
                        let _ = ui.add(
                            DatePickerButton::new(&mut self.departure_date)
                                .id_salt("departure_date_picker"),
                        );
                        ui.label("Time:");
                        ui.add(egui::DragValue::new(&mut self.departure_hour).range(0..=23));
                        ui.label(":");
                        ui.add(egui::DragValue::new(&mut self.departure_minute).range(0..=59));
                    });

                    ui.horizontal(|ui| {
                        ui.label("Arrival:");
                        let _ = ui.add(
                            DatePickerButton::new(&mut self.arrival_date)
                                .id_salt("arrival_date_picker"),
                        );
                        ui.label("Time:");
                        ui.add(egui::DragValue::new(&mut self.arrival_hour).range(0..=23));
                        ui.label(":");
                        ui.add(egui::DragValue::new(&mut self.arrival_minute).range(0..=59));
                    });

                    ui.horizontal(|ui| {
                        ui.label("Fuel level:");
                        ui.add(egui::DragValue::new(&mut self.fuel).speed(1).range(0..=100));
                        ui.label("%");
                    });

                    ui.horizontal(|ui| {
                        ui.label("Height:");
                        ui.add(
                            egui::DragValue::new(&mut self.height)
                                .speed(1)
                                .range(0..=100000),
                        );
                        ui.label(" meters");
                    });

                    ui.horizontal(|ui| {
                        ui.label("Speed:");
                        ui.add(
                            egui::DragValue::new(&mut self.speed)
                                .speed(1)
                                .range(0..=10000),
                        );
                        ui.label(" km/h");
                    });

                    let mut sorted_airports = airports.to_vec();
                    sorted_airports.sort_by(|a, b| a.iata.cmp(&b.iata));

                    ui.horizontal(|ui| {
                        ui.label("Origin:");
                        egui::ComboBox::from_id_salt("origin_combo")
                            .selected_text(&self.origin)
                            .show_ui(ui, |ui| {
                                for airport in sorted_airports.iter() {
                                    if ui
                                        .selectable_label(
                                            self.origin == airport.iata,
                                            &airport.iata,
                                        )
                                        .clicked()
                                    {
                                        self.origin = airport.iata.clone();
                                    }
                                }
                            });
                    });

                    ui.horizontal(|ui| {
                        ui.label("Destination:");
                        egui::ComboBox::from_id_salt("destination_combo")
                            .selected_text(&self.destination)
                            .show_ui(ui, |ui| {
                                for airport in sorted_airports.iter() {
                                    if ui
                                        .selectable_label(
                                            self.destination == airport.iata,
                                            &airport.iata,
                                        )
                                        .clicked()
                                    {
                                        self.destination = airport.iata.clone();
                                    }
                                }
                            });
                    });

                    if let Some(error) = &self.error_message {
                        ui.colored_label(egui::Color32::RED, error);
                    }

                    if ui.button("Submit").clicked() {
                        let mut errors = vec![];

                        if self.flight_number.is_empty() {
                            errors.push("Flight Number is required.");
                        }
                        if self.origin.is_empty() {
                            errors.push("Origin is required.");
                        }
                        if self.destination.is_empty() {
                            errors.push("Destination is required.");
                        } else if self.origin == self.destination {
                            errors.push("Origin cannot be the same as the destination.");
                        }
                        if self.fuel <= 0.0 {
                            errors.push("Fuel must be greater than 0.");
                        }
                        if self.height <= 0 {
                            errors.push("Height must be greater than 0.");
                        }
                        if self.speed <= 0 {
                            errors.push("Speed must be greater than 0.");
                        }
                        if self.departure_date < Utc::now().date_naive() {
                            errors.push("Departure time cannot be in the past.");
                        }
                        if self.arrival_date <= self.departure_date {
                            errors.push("Arrival time must be after departure time.");
                        }

                        if !errors.is_empty() {
                            self.error_message = Some(errors.join("\n"));
                        } else {
                            match db.add_flight(self.to_flight(airports)) {
                                Ok(_) => {
                                    self.error_message = None;
                                    should_close = true;
                                }
                                Err(_) => {
                                    self.error_message = Some(
                                        "Error: A flight with this number already exists."
                                            .to_string(),
                                    );
                                }
                            }
                        }
                    }
                });
            });

        self.is_open = is_open && !should_close;
        self.is_open
    }

    fn to_flight(&self, airports: &[Airport]) -> Flight {
        let pos_flight = airports
            .iter()
            .find(|airport| airport.iata == self.origin)
            .map(|airport| airport.position)
            .unwrap_or_else(|| Position::from_lat_lon(0.0, 0.0));

        let info = FlightInfo {
            number: self.flight_number.clone(),
            fuel: self.fuel,
            height: self.height,
            speed: self.speed,
            origin: self.origin.clone(),
            destination: self.destination.clone(),
        };

        let departure_time = self
            .departure_date
            .and_hms_opt(self.departure_hour, self.departure_minute, 0)
            .unwrap_or_default();

        let arrival_time = self
            .arrival_date
            .and_hms_opt(self.arrival_hour, self.arrival_minute, 0)
            .unwrap_or_default();

        Flight {
            number: self.flight_number.clone(),
            status: FlightStatus::Scheduled.as_str().to_string(),
            position: pos_flight,
            departure_time: departure_time.and_utc().timestamp(),
            arrival_time: arrival_time.and_utc().timestamp(),
            airport: Default::default(),
            direction: Default::default(),
            heading: 0.0,
            info: Some(info),
        }
    }
}
