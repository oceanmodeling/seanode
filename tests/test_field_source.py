import pathlib  
import sys
sys.path.append(str(pathlib.Path(__file__).parent.parent.absolute()))
import datetime
from seanode.field_source import FieldSource


# Define some FieldSource instances for testing.
fs_just_datetime = FieldSource(
    'dir/subdir/{yyyymmdd}/cwl.t{hh}z.nc', 
    [{'varname_out': 'cwl_bias_corrected', 'varname_file':'zeta', 'datum':'MSL'}], 
    {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'}
)

fs_no_datetime = FieldSource(
    'dir/subdir/cwl.t{lead}z.nc', 
    [{'varname_out': 'cwl_bias_corrected', 'varname_file':'zeta', 'datum':'MSL'}], 
    {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'}
)

fs_both = FieldSource(
    'dir/subdir/{yyyymmdd}/cwl.t{hh}z.{lead}.nc', 
    [{'varname_out': 'cwl_bias_corrected', 'varname_file':'zeta', 'datum':'MSL'}], 
    {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'}
)

# Define a common initialization datetime and namespace for testing.
init_datetime = datetime.datetime(2023, 1, 15, 6, 0)
namespace = {'lead':'003'}

# Define the tests.
# ----------------------------------------------------------------
def test_get_vars():
    vars_just_datetime = fs_just_datetime.get_vars()
    assert vars_just_datetime == ['cwl_bias_corrected']

def test_get_filename_just_datetime():
    filename = fs_just_datetime.get_filename(init_datetime)
    assert filename == 'dir/subdir/20230115/cwl.t06z.nc'

def test_get_filename_no_datetime():
    filename = fs_no_datetime.get_filename(init_datetime, namespace)
    assert filename == 'dir/subdir/cwl.t003z.nc'

def test_get_filename_no_datetime_with_none():
    filename = fs_no_datetime.get_filename(None, namespace)
    assert filename == 'dir/subdir/cwl.t003z.nc'

def test_get_filename_both():
    filename = fs_both.get_filename(init_datetime, namespace)
    assert filename == 'dir/subdir/20230115/cwl.t06z.003.nc'

def test_get_filename_missing_datetime():
    try:
        fs_just_datetime.get_filename(None)
    except KeyError as e:
        assert str(e) == "'yyyymmdd'"
    else:
        assert False, "Expected KeyError was not raised."

def test_get_filename_missing_namespace_key():
    try:
        fs_no_datetime.get_filename(init_datetime)
    except KeyError as e:
        assert str(e) == "'lead'"
    else:
        assert False, "Expected KeyError was not raised."


# Run the tests if this script is executed directly.
# ----------------------------------------------------------------
if __name__ == "__main__":
    test_get_vars()
    test_get_filename_just_datetime()
    test_get_filename_no_datetime()
    test_get_filename_no_datetime_with_none()
    test_get_filename_both()
    test_get_filename_missing_datetime()
    test_get_filename_missing_namespace_key()
    print("All tests passed.")
