use jetstreamer::JetstreamerRunner;
use jetstreamer_plugin::plugins::program_tracking::ProgramTrackingPlugin;
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Production-ready configuration
    let log_level = env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
    
    println!("🚀 Starting Solana Analytics Platform Streamer");
    println!("📊 Log Level: {}", log_level);
    
    JetstreamerRunner::default()
        .with_log_level(log_level)
        .parse_cli_args()?
        .with_plugin(Box::new(ProgramTrackingPlugin))
        // Add your custom plugins here:
        // .with_plugin(Box::new(DefiTrackingPlugin))
        // .with_plugin(Box::new(NftTrackingPlugin))
        .run()
        .map_err(|err| -> Box<dyn std::error::Error> { Box::new(err) })?;
    
    println!("✅ Streamer completed successfully");
    Ok(())
}
