"""Contains CLI tools to help with seanode usage.

Usage:
    python seanode_cli.py models
    python seanode_cli.py catalog <model_name>

"""


from seanode import request_options, model_factory
import argparse


def list_models():
    """List available models."""
    print("Available models:")
    for m in list(request_options.ModelOptions):
        print(f" - {m.name}")
    return None


def show_model_catalog(model_name: str):
    """Show the data catalog for a given model."""
    model_opts = [m.name for m in list(request_options.ModelOptions)]
    if model_name in model_opts:
        req_model = request_options.ModelOptions[model_name]
    else:
        raise ValueError(f'model {model_name} not recognized. Try one of {model_opts}.')
    # Get the model task creator.
    model = model_factory.get_model(req_model)
    catalog = model.data_catalog
    print(f"\n{model_name} data catalog:\n")
    for v in catalog:
        points_vars = []
        mesh_vars = []
        grid_vars = []
        for fs in catalog[v]['field_sources']:
            if fs.file_geometry == request_options.FileGeometry.POINTS:
                points_vars.extend(fs.get_vars())
            elif fs.file_geometry == request_options.FileGeometry.MESH:
                mesh_vars.extend(fs.get_vars())
            elif fs.file_geometry == request_options.FileGeometry.GRID:
                grid_vars.extend(fs.get_vars())
        print(v)
        print('----------')
        lr = catalog[v]['last_run']
        if lr is not None:
            lr = lr.strftime('%Y-%m-%d %H%M UTC')
        else:
            lr = 'present'
        fr = catalog[v]['first_run'].strftime('%Y-%m-%d %H%M UTC')
        print(f"Dates: {fr} to {lr}")
        if points_vars:
            print(f"Points variables: {points_vars}")
        if mesh_vars:
            print(f"Mesh variables: {mesh_vars}")
        if grid_vars:
            print(f"Grid variables: {grid_vars}")
        print("\n")
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SeaNode CLI Tools")
    subparsers = parser.add_subparsers(dest="command")

    # Subparser for listing models
    subparsers.add_parser("models", help="List available models")

    # Subparser for showing model catalog
    catalog_parser = subparsers.add_parser("catalog", help="Show data catalog for a model")
    catalog_parser.add_argument("model_name", type=str, help="Name of the model")

    args = parser.parse_args()

    if args.command == "models":
        list_models()
    elif args.command == "catalog":
        show_model_catalog(args.model_name)
    else:
        parser.print_help()