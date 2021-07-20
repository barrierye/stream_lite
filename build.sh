python setup.py bdist_wheel
python -m pip uninstall stream_lite -y
python -m pip install dist/stream_lite-0.0.0-py3-none-any.whl
git log --author="barrierye" --pretty=tformat: --numstat | awk '{ add += $1; subs += $2; loc += $1 - $2 } END { printf "added lines: %s, removed lines: %s, total lines: %s\n", add, subs, loc }'
