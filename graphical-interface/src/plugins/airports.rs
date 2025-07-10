use std::{cell::RefCell, rc::Rc};

use egui::{include_image, Image, Rect, Response, Vec2};
use walkers::{extras::Style, Plugin, Projector};

use crate::{state::SelectionState, types::Airport};

pub struct Airports<'a> {
    airports: &'a Vec<Airport>,
    selection_state: Rc<RefCell<SelectionState>>,
}

impl<'a> Airports<'a> {
    pub fn new(airports: &'a Vec<Airport>, selection_state: Rc<RefCell<SelectionState>>) -> Self {
        Self {
            airports,
            selection_state,
        }
    }
}

impl Plugin for Airports<'_> {
    fn run(self: Box<Self>, ui: &mut egui::Ui, _response: &Response, projector: &Projector) {
        for airport in self.airports {
            let mut style = Style::default();
            style.symbol_font.size = 24.;
            airport.draw(ui, projector, style, &mut self.selection_state.borrow_mut());
        }
    }
}

impl Airport {
    fn draw(
        &self,
        ui: &mut egui::Ui,
        projector: &Projector,
        _style: Style,
        selection_state: &mut SelectionState,
    ) {
        let screen_position = projector.project(self.position);

        let symbol_size = Vec2::new(30.0, 30.0);

        // let rect = Rect::from_center_size(screen_position.to_pos2(), symbol_size);
        let rect = {
            let min_pos =
                screen_position.to_pos2() - Vec2::new(symbol_size.x / 2.0, symbol_size.y - 15.0);
            Rect::from_min_size(min_pos, symbol_size)
        };

        let clickable_area = Rect::from_center_size(screen_position.to_pos2(), symbol_size);

        let response = ui.allocate_rect(clickable_area, egui::Sense::click());

        let image = if response.hovered() {
            Image::new(include_image!(
                r"../../../graphical-interface/location-pin-selected.svg"
            ))
        } else {
            Image::new(include_image!(
                r"../../../graphical-interface/location-pin-solid.svg"
            ))
        }
        .fit_to_exact_size(symbol_size);

        ui.put(rect, image);

        if response.clicked() {
            selection_state.toggle_airport_selection(self);
        }
    }
}
