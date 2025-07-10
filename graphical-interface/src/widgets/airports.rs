use egui::Widget;
use egui_extras::{Column, TableBuilder};

use crate::state::{SelectionState, ViewState};

/// Shows a list of the currently visible airports.
pub struct WidgetAirports<'a, 'b> {
    pub view_state: &'a ViewState,
    pub selection_state: &'b mut SelectionState,
}

impl<'a, 'b> WidgetAirports<'a, 'b> {
    pub fn new(view_state: &'a ViewState, selection_state: &'b mut SelectionState) -> Self {
        Self {
            view_state,
            selection_state,
        }
    }
}

impl Widget for WidgetAirports<'_, '_> {
    fn ui(self, ui: &mut egui::Ui) -> egui::Response {
        let response = ui.allocate_response(egui::vec2(0., 0.), egui::Sense::hover());

        egui::Window::new("Aeropuertos")
            .resizable(false)
            .movable(false)
            .collapsible(false)
            .fixed_pos([20., 20.])
            .show(ui.ctx(), |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| {
                    TableBuilder::new(ui)
                        .column(Column::auto())
                        .column(Column::remainder())
                        .sense(egui::Sense::click())
                        .header(20.0, |mut header| {
                            header.col(|ui| {
                                ui.strong("Code");
                            });
                            header.col(|ui| {
                                ui.strong("Name");
                            });
                        })
                        .body(|mut body| {
                            for airport in &self.view_state.airports {
                                body.row(18.0, |mut row| {
                                    row.set_selected({
                                        self.selection_state
                                            .airport
                                            .as_ref()
                                            .is_some_and(|a| *a == *airport)
                                    });

                                    row.col(|ui| {
                                        ui.label(&airport.iata);
                                    });

                                    row.col(|ui| {
                                        ui.label(&airport.name);
                                    });

                                    if row.response().clicked() {
                                        self.selection_state.toggle_airport_selection(&airport);
                                    }
                                });
                            }
                        });
                });
            });

        response
    }
}
