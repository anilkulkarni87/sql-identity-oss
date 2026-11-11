# VHS tape: records a full IDR run against Databricks.
#
# Prereqs:  brew install vhs
#           ~/.databrickscfg (host, http_path, token) or DATABRICKS_* env vars
#           idr_meta already configured (via `idr ui` wizard or `idr apply-config`)
#
# Record:   vhs docs/assets/recordings/idr-databricks-run.tape
# Output:   docs/assets/recordings/idr-databricks-run.gif (+ .mp4)

Output docs/assets/recordings/idr-databricks-run.gif
Output docs/assets/recordings/idr-databricks-run.mp4

Set Shell "zsh"
Set FontSize 15
Set Width 1280
Set Height 760
Set Padding 24
Set Theme "Catppuccin Mocha"
Set TypingSpeed 45ms
Set WindowBar Colorful

# --- hidden setup: clean prompt, silence driver chatter ---------------------
Hide
Type "export PS1='$ ' IDR_DRIVER_LOGS=0 && clear"
Enter
Show

# --- 1. show detected credentials -------------------------------------------
Type "# Credentials auto-discovered from ~/.databrickscfg"
Enter
Sleep 600ms
Type `python -c "from idr_core.credential_discovery import discover_all; [print(c['platform'].ljust(11), c['masked_summary']) for c in discover_all() if c['platform']=='databricks']"`
Enter
Sleep 4s

# --- 2. full identity resolution run ----------------------------------------
Type "idr run --platform databricks --mode FULL --strict"
Enter
# Adjust to your warehouse run time (watch the stage logs finish)
Sleep 296s

Sleep 5s

