gunicorn main:app -b 0.0.0.0:5000 --workers $((2*$(grep -c ^processor /proc/cpuinfo))) --log-level debug
