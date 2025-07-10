use crate::{db::Db, types::Airport};

use super::{flights_table::FlightType, View, WidgetFlightsTable};

/// Represents the selectable tabs for displaying flight information in the airport widget.
///
#[derive(PartialEq)]
enum Tabs {
    Departures,
    Arrivals,
}

/// A widget for displaying information about a specific airport.
///
/// This widget includes airport details (such as its IATA code and country) and
/// provides functionality to view flights categorized into departures and arrivals.
pub struct WidgetAirport {
    pub selected_airport: Airport,
    widget_departures: WidgetFlightsTable,
    widget_arrivals: WidgetFlightsTable,
    open_tab: Tabs,
}

impl WidgetAirport {
    /// Creates a new `WidgetAirport` for a given airport.
    pub fn new(selected_airport: Airport) -> Self {
        let iata_code = selected_airport.iata.clone();
        Self {
            selected_airport,
            open_tab: Tabs::Departures,
            widget_arrivals: WidgetFlightsTable::new(iata_code.clone(), FlightType::Arrival),
            widget_departures: WidgetFlightsTable::new(iata_code, FlightType::Departure),
        }
    }
}

impl WidgetAirport {
    /// This method shows a window with details about the selected airport,
    /// including a selector for viewing either departure or arrival flights.
    pub fn show(&mut self, ctx: &egui::Context, db: &mut Db) -> bool {
        let mut open = true;

        egui::Window::new(format!("Aeropuerto {}", self.selected_airport.name))
            .resizable(false)
            .collapsible(true)
            .open(&mut open)
            .fixed_pos([20.0, 20.0])
            .show(ctx, |ui| {
                ui.add_space(10.0);

                ui.visuals_mut().override_text_color = Some(egui::Color32::WHITE);
                ui.visuals_mut().widgets.noninteractive.bg_fill = egui::Color32::from_gray(30);
                ui.vertical(|ui| {
                    ui.label(
                        egui::RichText::new(format!("Código IATA: {}", self.selected_airport.iata))
                            .size(16.0),
                    );
                    ui.label(
                        egui::RichText::new(format!("País: {}", self.selected_airport.country))
                            .size(16.0),
                    );
                });

                ui.add_space(15.0);

                ui.horizontal(|ui| {
                    ui.label(
                        egui::RichText::new("Información de vuelos en:")
                            .size(18.0)
                            .strong(),
                    );
                    egui::ComboBox::from_label("")
                        .selected_text(match self.open_tab {
                            Tabs::Departures => "Salida",
                            Tabs::Arrivals => "Llegada",
                        })
                        .show_ui(ui, |ui| {
                            ui.selectable_value(&mut self.open_tab, Tabs::Departures, "Salidas");
                            ui.selectable_value(&mut self.open_tab, Tabs::Arrivals, "Llegadas");
                        });
                });

                ui.add_space(10.0);

                match self.open_tab {
                    Tabs::Departures => ui.vertical_centered(|ui| {
                        self.widget_departures.ui(ui, db);
                    }),
                    Tabs::Arrivals => ui.vertical_centered(|ui| {
                        self.widget_arrivals.ui(ui, db);
                    }),
                }
            });

        open
    }
}
