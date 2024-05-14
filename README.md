# newday-task-jl

I wanted to have a play with 

# Build and run
`make build`

Then for a single node
`make run`

Or for cluster mode
`make run-scale`

# Development
Submit Python jobs with the command:
`make submit app=dir/relative/to/spark_apps/dir`
e.g.
`make submit app=task.py`

For speed, copy a file into a running master container without having to re-up docker
`docker cp -L spark_apps/task.py da-spark-master:/opt/spark/apps/task.py`

# Test
I can't remember how the .toml piece works for forcing pytest paths so run like this from root
`python -m pytest`

## Dependency management
_Copied, as always, from the pip compile website_
This project uses [pip-compile-multi](https://pypi.org/project/pip-compile-multi/) for hard-pinning dependencies versions.
Please see its documentation for usage instructions.
In short, `requirements/base.in` contains the list of direct requirements with occasional version constraints (like `Django<2`)
and `requirements/base.txt` is automatically generated from it by adding recursive tree of dependencies with fixed versions.
The same goes for `test` and `dev`.

To upgrade dependency versions, run `pip-compile-multi`.

To add a new dependency without upgrade, add it to `requirements/base.in` and run `pip-compile-multi --no-upgrade`.

For installation always use `.txt` files. For example, command `pip install -Ue . -r requirements/dev.txt` will install
this project in development mode, testing requirements and development tools.
Another useful command is `pip-sync requirements/dev.txt`, it uninstalls packages from your virtualenv that aren't listed in the file.
