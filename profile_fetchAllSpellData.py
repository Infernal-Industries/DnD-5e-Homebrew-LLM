import subprocess
import cProfile
import pstats
import io
import os

def run_main_script():
    """Runs the main spell-fetching script and profiles it."""
    # Define the path to your main script
    script_path = "fetchAllSpellData.py"

    # Run the main script with profiling enabled
    profiler = cProfile.Profile()
    profiler.enable()
    subprocess.run(["python3", script_path], check=True)
    profiler.disable()

    # Create a Stats object and sort it
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')

    # Print the stats
    ps.print_stats()

    # Optionally save the profiling results to a file
    with open("profiling_results.txt", "w") as f:
        f.write(s.getvalue())

    print("Profiling complete. Results saved to profiling_results.txt")

def visualize_profiling():
    """Visualizes the profiling results using SnakeViz."""
    # Check if snakeviz is installed
    try:
        import snakeviz
    except ImportError:
        print("SnakeViz is not installed. Install it by running 'pip3 install snakeviz'")
        return

    # Run snakeviz on the profiling results
    os.system("snakeviz profiling_results.txt")

if __name__ == "__main__":
    run_main_script()
    visualize_profiling()
