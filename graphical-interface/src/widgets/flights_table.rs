use super::View;
use crate::{
    db::{Db, Provider},
    types::{Flight, FlightStatus},
};
use egui_extras::{Column, TableBuilder};

pub enum FlightType {
    Arrival,
    Departure,
}

pub struct WidgetFlightsTable {
    airport: String,
    selected_date: chrono::NaiveDate,
    flights: Option<Vec<Flight>>,
    flight_type: FlightType,
    edit_mode: bool, // Edit mode toggle
    edited_flight_states: std::collections::HashMap<String, FlightStatus>, // Track edited flight states
}

impl WidgetFlightsTable {
    pub fn new(airport: String, flight_type: FlightType) -> Self {
        Self {
            airport,
            selected_date: chrono::offset::Utc::now().date_naive(),
            flights: None,
            flight_type,
            edit_mode: false, // Default edit mode is off
            edited_flight_states: std::collections::HashMap::new(),
        }
    }

    fn fetch_flights(&mut self, db: &mut Db) {
        self.flights = match self.flight_type {
            FlightType::Arrival => {
                match db.get_arrival_flights(&self.airport, self.selected_date) {
                    Ok(flights) => Some(flights),
                    Err(_) => {
                        eprintln!("Error fetching arrival flights");
                        None
                    }
                }
            }
            FlightType::Departure => {
                match db.get_departure_flights(&self.airport, self.selected_date) {
                    Ok(flights) => Some(flights),
                    Err(_) => {
                        eprintln!("Error fetching departure flights");
                        None
                    }
                }
            }
        };
    }

    fn allowed_transitions(current_status: &FlightStatus) -> Vec<FlightStatus> {
        match current_status {
            FlightStatus::Scheduled => vec![FlightStatus::Finished, FlightStatus::Canceled],
            FlightStatus::OnTime => vec![
                FlightStatus::Delayed,
                FlightStatus::Finished,
                FlightStatus::Canceled,
            ],
            FlightStatus::Delayed => vec![FlightStatus::Finished, FlightStatus::Canceled],
            FlightStatus::Finished | FlightStatus::Canceled => {
                vec![FlightStatus::Finished, FlightStatus::Canceled]
            }
        }
    }
}

impl View for WidgetFlightsTable {
    fn ui(&mut self, ui: &mut egui::Ui, db: &mut Db) {
        if self.flights.is_none() {
            self.fetch_flights(db);
        }

        ui.vertical_centered(|ui| {
            ui.horizontal(|ui| {
                ui.label(
                    egui::RichText::new("Fecha:")
                        .size(16.0)
                        .strong()
                        .color(egui::Color32::WHITE),
                );
                let date_response =
                    ui.add(egui_extras::DatePickerButton::new(&mut self.selected_date));

                if date_response.changed() {
                    self.fetch_flights(db);
                }
            });

            ui.add_space(10.0);

            ui.checkbox(&mut self.edit_mode, "Edit Mode");

            ui.add_space(10.0);

            let flights = self.flights.clone(); //Requerido por como funciona egui.

            if let Some(flights) = flights {
                ui.group(|ui| {
                    TableBuilder::new(ui)
                        .striped(true)
                        .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
                        .column(Column::remainder().at_least(100.0)) // Flight number column
                        .column(Column::remainder().at_least(100.0)) // Status column
                        .column(Column::remainder().at_least(50.0)) // Save button column
                        .header(25.0, |mut header| {
                            header.col(|ui| {
                                ui.strong(
                                    egui::RichText::new("Vuelo")
                                        .color(egui::Color32::YELLOW)
                                        .size(16.0),
                                );
                            });
                            header.col(|ui| {
                                ui.strong(
                                    egui::RichText::new("Estado")
                                        .color(egui::Color32::YELLOW)
                                        .size(16.0),
                                );
                            });
                            if self.edit_mode {
                                header.col(|ui| {
                                    ui.strong(
                                        egui::RichText::new("Acción")
                                            .color(egui::Color32::YELLOW)
                                            .size(16.0),
                                    );
                                });
                            }
                        })
                        .body(|mut body| {
                            for flight in flights {
                                body.row(20.0, |mut row| {
                                    row.col(|ui| {
                                        ui.label(
                                            egui::RichText::new(&flight.number)
                                                .color(egui::Color32::WHITE)
                                                .size(14.0),
                                        );
                                    });

                                    row.col(|ui| {
                                        if self.edit_mode {
                                            let current_state =
                                                FlightStatus::from_str(&flight.status)
                                                    .unwrap_or(FlightStatus::Scheduled);
                                            let available_states =
                                                WidgetFlightsTable::allowed_transitions(
                                                    &current_state,
                                                );

                                            let edited_state = self
                                                .edited_flight_states
                                                .entry(flight.number.clone())
                                                .or_insert(current_state.clone());

                                            egui::ComboBox::from_id_salt(&flight.number)
                                                .selected_text(edited_state.as_str())
                                                .show_ui(ui, |ui| {
                                                    for state in available_states {
                                                        if ui
                                                            .selectable_label(
                                                                *edited_state == state,
                                                                state.as_str(),
                                                            )
                                                            .clicked()
                                                        {
                                                            *edited_state = state;
                                                        }
                                                    }
                                                });
                                        } else {
                                            ui.label(
                                                egui::RichText::new(&flight.status)
                                                    .color(egui::Color32::WHITE)
                                                    .size(14.0),
                                            );
                                        }
                                    });

                                    // Save button (only in edit mode)
                                    if self.edit_mode {
                                        row.col(|ui| {
                                            if ui.button("Save").clicked() {
                                                if let Some(new_status) =
                                                    self.edited_flight_states.get(&flight.number)
                                                {
                                                    let direction = match self.flight_type {
                                                        FlightType::Arrival => "ARRIVAL",
                                                        FlightType::Departure => "DEPARTURE",
                                                    };

                                                    let mut updated_flight = flight.clone();
                                                    updated_flight.status =
                                                        new_status.as_str().to_string();
                                                    match db.update_state(updated_flight, direction)
                                                    {
                                                        Ok(_) => {
                                                            // Refresh flights and clear the edited state
                                                            self.fetch_flights(db);
                                                            self.edited_flight_states
                                                                .remove(&flight.number);
                                                        }
                                                        Err(_) => {
                                                            ui.label(
                                                                egui::RichText::new(
                                                                    "Failed to update.",
                                                                )
                                                                .color(egui::Color32::RED),
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                        });
                                    }
                                });
                            }
                        });
                });
            } else {
                ui.label("No hay vuelos.");
            }
        });
    }
}