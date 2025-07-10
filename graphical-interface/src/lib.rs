use db::Db;

pub mod db;
mod map;
mod plugins;
mod state;
mod types;
mod widgets;
mod windows;
use map::MyApp;

pub fn run() -> Result<(), eframe::Error> {
    eframe::run_native(
        "Flight Tracker",
        Default::default(),
        Box::new(|cc| Ok(Box::new(MyApp::new(cc.egui_ctx.clone(), Db::new())))),
    )
}
