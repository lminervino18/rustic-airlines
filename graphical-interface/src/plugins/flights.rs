use std::{cell::RefCell, rc::Rc};

use egui::{include_image, Image, Rect, Response, Vec2};
use walkers::{extras::Style, Plugin, Projector};

use crate::{state::SelectionState, types::Flight};

pub struct Flights<'a> {
    flights: &'a Vec<Flight>,
    selection_state: Rc<RefCell<SelectionState>>,
}

impl<'a> Flights<'a> {
    pub fn new(flights: &'a Vec<Flight>, selection_state: Rc<RefCell<SelectionState>>) -> Self {
        Self {
            flights,
            selection_state,
        }
    }
}

impl Plugin for Flights<'_> {
    fn run(self: Box<Self>, ui: &mut egui::Ui, _response: &Response, projector: &Projector) {
        for flight in self.flights {
            let mut style = Style::default();
            style.symbol_font.size = 24.;
            flight.draw(ui, projector, style, &mut self.selection_state.borrow_mut());
        }
    }
}

impl Flight {
    fn draw(
        &self,
        ui: &mut egui::Ui,
        projector: &Projector,
        _style: Style,
        selection_state: &mut SelectionState,
    ) {
        let screen_position = projector.project(self.position);

        // Define the size for the plane icon
        let symbol_size = Vec2::new(30.0, 30.0);

        let clickable_area = Rect::from_center_size(screen_position.to_pos2(), symbol_size);

        let response = ui.allocate_rect(clickable_area, egui::Sense::click());

        // Calculate the rectangle where the image should be drawn
        let rect = Rect::from_center_size(screen_position.to_pos2(), symbol_size);

        let image = if response.hovered() {
            Image::new(include_image!(r"../../plane-solid-selected.svg"))
        } else {
            Image::new(include_image!(r"../../plane-solid.svg"))
        };

        let image = image
            .fit_to_exact_size(symbol_size)
            .rotate(self.heading.to_radians(), Vec2::splat(0.5));

        ui.put(rect, image);

        if response.clicked() {
            selection_state.toggle_flight_selection(self);
        }
    }
}
