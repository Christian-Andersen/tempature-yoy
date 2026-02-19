let chart = null;
let weatherData = null;

async function init() {
    // Relative path for GitHub Pages compatibility
    const response = await fetch('public/data.json');
    weatherData = await response.json();

    if (!weatherData) return;

    const locationSelect = document.getElementById('locationSelect');
    const metricSelect = document.getElementById('metricSelect');

    locationSelect.innerHTML = '';
    weatherData.metadata.locations.forEach(loc => {
        const option = document.createElement('option');
        option.value = loc;
        option.textContent = loc;
        locationSelect.appendChild(option);
    });

    locationSelect.addEventListener('change', updateChart);
    metricSelect.addEventListener('change', updateChart);

    updateChart();
}

function updateChart() {
    if (!weatherData) return;

    const location = document.getElementById('locationSelect').value;
    const metric = document.getElementById('metricSelect').value;
    const data = weatherData.locations[location];

    const ctx = document.getElementById('weatherChart').getContext('2d');
    
    if (chart) {
        chart.destroy();
    }

    const labels = data.labels;
    const prevYear = weatherData.metadata.prev_year.toString();
    const targetYear = weatherData.metadata.target_year.toString();

    // @ts-ignore
    chart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: prevYear,
                    data: data[metric][prevYear],
                    borderColor: 'rgba(54, 162, 235, 1)',
                    backgroundColor: 'rgba(54, 162, 235, 0.2)',
                    borderWidth: 2,
                    pointRadius: 0,
                    tension: 0.1
                },
                {
                    label: targetYear,
                    data: data[metric][targetYear],
                    borderColor: 'rgba(255, 99, 132, 1)',
                    backgroundColor: 'rgba(255, 99, 132, 0.2)',
                    borderWidth: 2,
                    pointRadius: 0,
                    tension: 0.1
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: false,
                    title: {
                        display: true,
                        text: metric === 'max_temp' ? 'Temperature (°C)' : 'Humidity (%)'
                    }
                },
                x: {
                    ticks: {
                        autoSkip: true,
                        maxRotation: 45,
                        minRotation: 45,
                        callback: function(val, index) {
                            const label = labels[index];
                            if (label.startsWith('01') || label.startsWith('15') || index === labels.length - 1) {
                                return label;
                            }
                            return null;
                        }
                    }
                }
            },
            plugins: {
                title: {
                    display: true,
                    text: `${location} - Rolling 14 Day Average`
                },
                tooltip: {
                    mode: 'index',
                    intersect: false
                }
            }
        }
    });
}

window.addEventListener('DOMContentLoaded', init);
