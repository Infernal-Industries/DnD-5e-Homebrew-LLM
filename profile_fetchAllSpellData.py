import subprocess
import cProfile
import pstats
import io

def run_main_script():
    """Runs the main spell-fetching script and profiles it."""
    # Define the path to your main script
    script_path = "fetchAllSpellData.py"

    # Run the main script with profiling enabled
    profiler = cProfile.Profile()
    profiler.enable()
    subprocess.run(["python3", script_path], check=True)
    profiler.disable()

    # Save the profiling results to a binary .prof file
    prof_filename = "profiling_results.prof"
    profiler.dump_stats(prof_filename)

    # Optionally print the stats to the console
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
    ps.print_stats()
    print(s.getvalue())

    print(f"Profiling complete. Results saved to {prof_filename}")

def visualize_profiling():
    """Visualizes the profiling results using SnakeViz."""
    try:
        # Use the .prof file with SnakeViz
        subprocess.run(["python3", "-m", "snakeviz", "profiling_results.prof"], check=True)
    except subprocess.CalledProcessError:
        print("Failed to run SnakeViz. Ensure SnakeViz is installed via pip3.")

if __name__ == "__main__":
    run_main_script()
    visualize_profiling()
