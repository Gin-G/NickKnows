import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
from scipy import stats
import json
import os
from pathlib import Path
import logging

# Set up matplotlib for server use
plt.ioff()  # Turn off interactive mode
logger = logging.getLogger(__name__)

def create_team_opportunity_plots(team, weekly_position_data, available_weeks, selected_year):
    """
    Create comprehensive opportunity plots for a team
    """
    try:
        # Set up plotting style
        plt.style.use('default')  # Use default style for compatibility
        
        # Create team plots directory
        plots_dir = Path(f'nickknows/static/images/opportunities/{team}/')
        plots_dir.mkdir(parents=True, exist_ok=True)
        
        plot_data = {}
        
        for position in ['QB', 'RB', 'WR', 'TE']:
            if position not in weekly_position_data:
                continue
                
            players_data = weekly_position_data[position]
            if not players_data:
                continue
                
            # Create position-specific plots
            position_plots = create_position_plots(
                position, players_data, available_weeks, team, selected_year, plots_dir
            )
            plot_data[position] = position_plots
        
        # Create team summary plots
        summary_plots = create_team_summary_plots(
            weekly_position_data, available_weeks, team, selected_year, plots_dir
        )
        plot_data['summary'] = summary_plots
        
        return plot_data
        
    except Exception as e:
        logger.error(f"Error creating plots for {team}: {str(e)}")
        return {}

def create_position_plots(position, players_data, available_weeks, team, selected_year, plots_dir):
    """Create plots for a specific position group"""
    plots = {}
    
    try:
        # Determine primary metric based on position
        primary_metric = 'targets' if position in ['QB', 'WR', 'TE'] else 'carries'
        
        # 1. Weekly Trends Plot
        plots['weekly_trends'] = create_weekly_trends_plot(
            position, players_data, available_weeks, primary_metric, team, selected_year, plots_dir
        )
        
        # 2. Target Share Evolution (for skill positions)
        if position in ['WR', 'TE', 'RB']:
            plots['target_share'] = create_target_share_plot(
                position, players_data, available_weeks, team, selected_year, plots_dir
            )
        
        # 3. Opportunity Correlation (targets vs carries for RB/WR/TE)
        if position in ['RB', 'WR', 'TE']:
            plots['correlation'] = create_opportunity_correlation_plot(
                position, players_data, available_weeks, team, selected_year, plots_dir
            )
        
        # 4. Red Zone Opportunities
        plots['red_zone'] = create_red_zone_opportunities_plot(
            position, players_data, available_weeks, team, selected_year, plots_dir
        )
        
    except Exception as e:
        logger.error(f"Error creating {position} plots: {str(e)}")
    
    return plots

def create_weekly_trends_plot(position, players_data, available_weeks, metric, team, selected_year, plots_dir):
    """Create weekly trends plot with 3-game and 5-game trend lines"""
    
    try:
        # Filter to top players by average
        top_players = sorted(players_data, key=lambda x: x.get(f'{metric}_avg', 0), reverse=True)[:8]
        
        if not top_players:
            return None
        
        # Create the plot
        fig, ax = plt.subplots(figsize=(14, 8))
        
        # Use a color palette
        colors = plt.cm.tab10(np.linspace(0, 1, len(top_players)))
        
        for i, player in enumerate(top_players):
            weekly_data = player.get(f'weekly_{metric}', {})
            weeks = []
            values = []
            
            for week in available_weeks:
                if week in weekly_data:
                    weeks.append(week)
                    values.append(weekly_data[week])
            
            if len(weeks) < 2:
                continue
                
            # Plot actual values
            ax.plot(weeks, values, 'o-', color=colors[i], label=player['player_name'][:15], 
                    linewidth=2.5, markersize=6, alpha=0.8)
            
            # Add 3-game trend line
            if len(weeks) >= 3:
                trend_3 = calculate_rolling_trend(weeks, values, window=3)
                if trend_3:
                    ax.plot(weeks[2:], trend_3, '--', color=colors[i], alpha=0.6, linewidth=2)
            
            # Add 5-game trend line
            if len(weeks) >= 5:
                trend_5 = calculate_rolling_trend(weeks, values, window=5)
                if trend_5:
                    ax.plot(weeks[4:], trend_5, ':', color=colors[i], alpha=0.5, linewidth=2.5)
        
        # Customize plot
        ax.set_xlabel('Week', fontsize=12, fontweight='bold')
        ax.set_ylabel(f'{metric.title()} per Game', fontsize=12, fontweight='bold')
        ax.set_title(f'{team} {position} {metric.title()} Trends ({selected_year})', 
                    fontsize=14, fontweight='bold', pad=20)
        
        # Set week ticks
        ax.set_xticks(available_weeks)
        ax.set_xlim(min(available_weeks) - 0.5, max(available_weeks) + 0.5)
        
        # Add grid
        ax.grid(True, alpha=0.3, linestyle='-', linewidth=0.5)
        
        # Add legend
        legend = ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9)
        legend.set_title('Players', prop={'weight': 'bold'})
        
        # Add trend line explanation
        ax.text(0.02, 0.98, 'Lines: — Actual  -- 3-game trend  ⋯ 5-game trend', 
                transform=ax.transAxes, fontsize=9, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        plt.tight_layout()
        
        # Save plot
        filename = f'{position}_{metric}_trends_{selected_year}.png'
        filepath = plots_dir / filename
        plt.savefig(filepath, dpi=300, bbox_inches='tight', facecolor='white')
        plt.close(fig)
        
        return str(filepath.relative_to('nickknows/static/'))
        
    except Exception as e:
        logger.error(f"Error creating red zone plot: {str(e)}")
        return None

def create_team_summary_plots(weekly_position_data, available_weeks, team, selected_year, plots_dir):
    """Create team-level summary plots"""
    plots = {}
    
    try:
        # 1. Team Opportunity Distribution
        plots['distribution'] = create_team_distribution_plot(
            weekly_position_data, available_weeks, team, selected_year, plots_dir
        )
        
        # 2. Weekly Team Totals
        plots['weekly_totals'] = create_weekly_totals_plot(
            weekly_position_data, available_weeks, team, selected_year, plots_dir
        )
        
    except Exception as e:
        logger.error(f"Error creating team summary plots: {str(e)}")
    
    return plots

def create_team_distribution_plot(weekly_position_data, available_weeks, team, selected_year, plots_dir):
    """Create team opportunity distribution plot"""
    
    try:
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        
        position_colors = {'QB': '#FF6B6B', 'RB': '#4ECDC4', 'WR': '#45B7D1', 'TE': '#FFA07A'}
        
        # Plot for each position
        for pos_idx, (position, ax) in enumerate(zip(['QB', 'RB', 'WR', 'TE'], [ax1, ax2, ax3, ax4])):
            if position not in weekly_position_data:
                ax.text(0.5, 0.5, f'No {position} Data', ha='center', va='center', 
                       transform=ax.transAxes, fontsize=14, style='italic')
                ax.set_title(f'{position} Opportunities', fontweight='bold', fontsize=14)
                ax.set_xticks([])
                ax.set_yticks([])
                continue
            
            players_data = weekly_position_data[position][:6]  # Top 6 players
            
            if not players_data:
                continue
            
            # Create stacked bar chart of opportunities
            player_names = [p['player_name'][:10] + '...' if len(p['player_name']) > 10 else p['player_name'] for p in players_data]
            targets_avg = [p.get('targets_avg', 0) for p in players_data]
            carries_avg = [p.get('carries_avg', 0) for p in players_data]
            
            x_pos = np.arange(len(player_names))
            
            bars1 = ax.bar(x_pos, targets_avg, label='Targets', alpha=0.8, 
                          color=position_colors[position])
            bars2 = ax.bar(x_pos, carries_avg, bottom=targets_avg, label='Carries', 
                          alpha=0.6, color=position_colors[position], edgecolor='white', linewidth=1)
            
            ax.set_title(f'{position} Opportunities', fontweight='bold', fontsize=14)
            ax.set_xticks(x_pos)
            ax.set_xticklabels(player_names, rotation=45, ha='right', fontsize=10)
            ax.set_ylabel('Avg per Game', fontweight='bold')
            
            if pos_idx == 0:  # Only show legend for first subplot
                ax.legend(loc='upper right')
            
            ax.grid(True, alpha=0.3, axis='y')
            
            # Add value labels on bars
            for i, (targets, carries) in enumerate(zip(targets_avg, carries_avg)):
                total = targets + carries
                if total > 0:
                    ax.text(i, total + 0.1, f'{total:.1f}', ha='center', va='bottom', 
                           fontweight='bold', fontsize=9)
        
        plt.suptitle(f'{team} Opportunity Distribution by Position ({selected_year})', 
                    fontsize=18, fontweight='bold', y=0.98)
        plt.tight_layout()
        
        filename = f'team_opportunity_distribution_{selected_year}.png'
        filepath = plots_dir / filename
        plt.savefig(filepath, dpi=300, bbox_inches='tight', facecolor='white')
        plt.close(fig)
        
        return str(filepath.relative_to('nickknows/static/'))
        
    except Exception as e:
        logger.error(f"Error creating team distribution plot: {str(e)}")
        return None

def create_weekly_totals_plot(weekly_position_data, available_weeks, team, selected_year, plots_dir):
    """Create weekly team totals plot"""
    
    try:
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
        
        # Calculate weekly team totals
        weekly_targets = {week: 0 for week in available_weeks}
        weekly_carries = {week: 0 for week in available_weeks}
        
        for position, players_data in weekly_position_data.items():
            for player in players_data:
                for week in available_weeks:
                    weekly_targets[week] += player.get('weekly_targets', {}).get(week, 0)
                    weekly_carries[week] += player.get('weekly_carries', {}).get(week, 0)
        
        weeks = list(weekly_targets.keys())
        targets = list(weekly_targets.values())
        carries = list(weekly_carries.values())
        
        # Plot 1: Weekly Targets
        ax1.plot(weeks, targets, 'o-', linewidth=3, markersize=8, color='#1f77b4', label='Targets')
        ax1.fill_between(weeks, targets, alpha=0.3, color='#1f77b4')
        
        # Add trend line
        if len(weeks) >= 3:
            try:
                z = np.polyfit(weeks, targets, 1)
                p = np.poly1d(z)
                trend_line = p(weeks)
                ax1.plot(weeks, trend_line, '--', color='red', linewidth=2, label='Trend', alpha=0.8)
                
                # Add trend annotation
                slope = z[0]
                trend_text = f"Trend: {'+' if slope > 0 else ''}{slope:.1f} targets/week"
                ax1.text(0.02, 0.98, trend_text, transform=ax1.transAxes, 
                        bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
                        fontweight='bold', verticalalignment='top')
            except:
                pass
        
        ax1.set_ylabel('Total Targets', fontweight='bold', fontsize=12)
        ax1.set_title('Weekly Team Targets', fontweight='bold', fontsize=14)
        ax1.grid(True, alpha=0.3)
        ax1.legend()
        
        # Plot 2: Weekly Carries
        ax2.plot(weeks, carries, 'o-', linewidth=3, markersize=8, color='#2ca02c', label='Carries')
        ax2.fill_between(weeks, carries, alpha=0.3, color='#2ca02c')
        
        # Add trend line
        if len(weeks) >= 3:
            try:
                z = np.polyfit(weeks, carries, 1)
                p = np.poly1d(z)
                trend_line = p(weeks)
                ax2.plot(weeks, trend_line, '--', color='red', linewidth=2, label='Trend', alpha=0.8)
                
                # Add trend annotation
                slope = z[0]
                trend_text = f"Trend: {'+' if slope > 0 else ''}{slope:.1f} carries/week"
                ax2.text(0.02, 0.98, trend_text, transform=ax2.transAxes, 
                        bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
                        fontweight='bold', verticalalignment='top')
            except:
                pass
        
        ax2.set_xlabel('Week', fontweight='bold', fontsize=12)
        ax2.set_ylabel('Total Carries', fontweight='bold', fontsize=12)
        ax2.set_title('Weekly Team Carries', fontweight='bold', fontsize=14)
        ax2.grid(True, alpha=0.3)
        ax2.legend()
        
        # Set x-axis for both plots
        for ax in [ax1, ax2]:
            ax.set_xticks(weeks)
            ax.set_xlim(min(weeks) - 0.5, max(weeks) + 0.5)
            
            # Add week labels
            ax.set_xticklabels([f'W{w}' for w in weeks])
        
        plt.suptitle(f'{team} Weekly Opportunity Totals ({selected_year})', 
                    fontsize=16, fontweight='bold')
        plt.tight_layout()
        
        filename = f'team_weekly_totals_{selected_year}.png'
        filepath = plots_dir / filename
        plt.savefig(filepath, dpi=300, bbox_inches='tight', facecolor='white')
        plt.close(fig)
        
        return str(filepath.relative_to('nickknows/static/'))
        
    except Exception as e:
        logger.error(f"Error creating weekly totals plot: {str(e)}")
        return None

def calculate_rolling_trend(weeks, values, window=3):
    """Calculate rolling trend line using linear regression"""
    if len(weeks) < window:
        return []
    
    trend_values = []
    
    try:
        for i in range(window - 1, len(weeks)):
            window_weeks = weeks[i - window + 1:i + 1]
            window_values = values[i - window + 1:i + 1]
            
            # Linear regression for the window
            if len(set(window_values)) > 1:  # Avoid regression on constant values
                slope, intercept, _, _, _ = stats.linregress(window_weeks, window_values)
                trend_value = slope * weeks[i] + intercept
            else:
                trend_value = window_values[-1]
            
            trend_values.append(trend_value)
            
    except Exception as e:
        logger.error(f"Error calculating rolling trend: {str(e)}")
        return []
    
    return trend_values

def safe_create_plots_directory(team):
    """Safely create plots directory"""
    try:
        plots_dir = Path(f'nickknows/static/images/opportunities/{team}/')
        plots_dir.mkdir(parents=True, exist_ok=True)
        return plots_dir
    except Exception as e:
        logger.error(f"Error creating plots directory: {str(e)}")
        return None

def cleanup_old_plots(team, selected_year, keep_days=7):
    """Clean up old plot files to save space"""
    try:
        plots_dir = Path(f'nickknows/static/images/opportunities/{team}/')
        if not plots_dir.exists():
            return
        
        import time
        current_time = time.time()
        cutoff_time = current_time - (keep_days * 24 * 60 * 60)  # Convert days to seconds
        
        for file_path in plots_dir.glob('*.png'):
            try:
                # Keep files from current year
                if str(selected_year) in file_path.name:
                    continue
                    
                # Remove old files
                if file_path.stat().st_mtime < cutoff_time:
                    file_path.unlink()
                    logger.info(f"Removed old plot file: {file_path}")
            except Exception as e:
                logger.warning(f"Could not remove old plot file {file_path}: {str(e)}")
                
    except Exception as e:
        logger.error(f"Error cleaning up old plots: {str(e)}")

# Utility function to validate plot data
def validate_plot_data(weekly_position_data, available_weeks):
    """Validate data before plotting"""
    issues = []
    try:
        if not weekly_position_data:
            issues.append("No position data available")
            return issues
        
        if not available_weeks:
            issues.append("No weeks available")
            return issues
        
        for position, players_data in weekly_position_data.items():
            if not players_data:
                issues.append(f"No players data for {position}")
                continue
                
            for player in players_data:
                # Check required fields
                required_fields = ['player_name', 'weekly_targets', 'weekly_carries']
                missing_fields = [field for field in required_fields if field not in player]
                if missing_fields:
                    issues.append(f"Missing fields for {player.get('player_name', 'Unknown')}: {missing_fields}")
        
        return issues.relative_to('nickknows/static/')
        
    except Exception as e:
        logger.error(f"Error creating weekly trends plot: {str(e)}")
        return None

def create_target_share_plot(position, players_data, available_weeks, team, selected_year, plots_dir):
    """Create target share evolution plot"""
    
    try:
        # Filter to players with meaningful target share
        significant_players = [p for p in players_data if p.get('target_share_avg', 0) >= 5.0][:6]
        
        if not significant_players:
            return None
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        colors = plt.cm.viridis(np.linspace(0, 1, len(significant_players)))
        
        for i, player in enumerate(significant_players):
            weekly_share = player.get('weekly_target_share', {})
            weeks = []
            shares = []
            
            for week in available_weeks:
                if week in weekly_share:
                    weeks.append(week)
                    shares.append(weekly_share[week])
            
            if len(weeks) < 2:
                continue
            
            # Plot with filled area
            ax.plot(weeks, shares, 'o-', color=colors[i], label=player['player_name'][:15],
                    linewidth=2.5, markersize=5)
            ax.fill_between(weeks, shares, alpha=0.2, color=colors[i])
        
        ax.set_xlabel('Week', fontsize=12, fontweight='bold')
        ax.set_ylabel('Target Share (%)', fontsize=12, fontweight='bold')
        ax.set_title(f'{team} {position} Target Share Evolution ({selected_year})', 
                    fontsize=14, fontweight='bold')
        
        ax.set_xticks(available_weeks)
        ax.set_ylim(0, max(100, ax.get_ylim()[1]))
        ax.grid(True, alpha=0.3)
        
        legend = ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9)
        legend.set_title('Players', prop={'weight': 'bold'})
        
        plt.tight_layout()
        
        filename = f'{position}_target_share_{selected_year}.png'
        filepath = plots_dir / filename
        plt.savefig(filepath, dpi=300, bbox_inches='tight', facecolor='white')
        plt.close(fig)
        
        return str(filepath.relative_to('nickknows/static/'))
        
    except Exception as e:
        logger.error(f"Error creating target share plot: {str(e)}")
        return None

def create_opportunity_correlation_plot(position, players_data, available_weeks, team, selected_year, plots_dir):
    """Create targets vs carries correlation plot"""
    
    try:
        fig, ax = plt.subplots(figsize=(10, 8))
        
        colors = plt.cm.tab10(np.linspace(0, 1, min(10, len(players_data))))
        
        for i, player in enumerate(players_data[:10]):  # Top 10 players
            targets_data = []
            carries_data = []
            weeks_data = []
            
            for week in available_weeks:
                targets = player.get('weekly_targets', {}).get(week, 0)
                carries = player.get('weekly_carries', {}).get(week, 0)
                
                if targets > 0 or carries > 0:  # Only include weeks with activity
                    targets_data.append(targets)
                    carries_data.append(carries)
                    weeks_data.append(week)
            
            if len(targets_data) < 2:
                continue
            
            # Create scatter plot with week progression
            scatter = ax.scatter(targets_data, carries_data, s=[w*15 for w in weeks_data], 
                               alpha=0.6, label=player['player_name'][:12], color=colors[i])
            
            # Add trend line if enough data points
            if len(targets_data) >= 3:
                try:
                    z = np.polyfit(targets_data, carries_data, 1)
                    p = np.poly1d(z)
                    x_trend = np.linspace(min(targets_data), max(targets_data), 100)
                    ax.plot(x_trend, p(x_trend), '--', alpha=0.5, color=colors[i])
                except:
                    pass  # Skip trend line if polynomial fit fails
        
        ax.set_xlabel('Targets per Game', fontsize=12, fontweight='bold')
        ax.set_ylabel('Carries per Game', fontsize=12, fontweight='bold')
        ax.set_title(f'{team} {position} Opportunity Correlation ({selected_year})', 
                    fontsize=14, fontweight='bold')
        
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        filename = f'{position}_opportunity_correlation_{selected_year}.png'
        filepath = plots_dir / filename
        plt.savefig(filepath, dpi=300, bbox_inches='tight', facecolor='white')
        plt.close(fig)
        
        return str(filepath.relative_to('nickknows/static/'))
        
    except Exception as e:
        logger.error(f"Error creating correlation plot: {str(e)}")
        return None

def create_red_zone_opportunities_plot(position, players_data, available_weeks, team, selected_year, plots_dir):
    """Create red zone opportunities plot"""
    
    try:
        # Filter players with red zone opportunities
        rz_players = [p for p in players_data if p.get('red_zone_avg', 0) >= 0.5][:8]
        
        if not rz_players:
            return None
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # Plot 1: Weekly Red Zone Opportunities
        colors = plt.cm.Set2(np.linspace(0, 1, len(rz_players)))
        
        for i, player in enumerate(rz_players):
            weekly_rz = player.get('weekly_red_zone', {})
            weeks = []
            rz_opps = []
            
            for week in available_weeks:
                weeks.append(week)
                rz_opps.append(weekly_rz.get(week, 0))
            
            ax1.plot(weeks, rz_opps, 'o-', color=colors[i], label=player['player_name'][:12],
                    linewidth=2, markersize=5)
        
        ax1.set_xlabel('Week', fontweight='bold')
        ax1.set_ylabel('Red Zone Opportunities', fontweight='bold')
        ax1.set_title('Weekly Red Zone Opportunities', fontweight='bold')
        ax1.set_xticks(available_weeks)
        ax1.grid(True, alpha=0.3)
        ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
        
        # Plot 2: Red Zone Opportunity Distribution
        player_names = [p['player_name'][:12] for p in rz_players]  # Truncate long names
        rz_averages = [p.get('red_zone_avg', 0) for p in rz_players]
        rz_maxes = [p.get('red_zone_max', 0) for p in rz_players]
        
        x_pos = np.arange(len(player_names))
        bars1 = ax2.bar(x_pos, rz_averages, alpha=0.7, label='Average', color=colors[:len(player_names)])
        bars2 = ax2.bar(x_pos, rz_maxes, alpha=0.4, label='Max', color=colors[:len(player_names)])
        
        ax2.set_xlabel('Players', fontweight='bold')
        ax2.set_ylabel('Red Zone Opportunities', fontweight='bold')
        ax2.set_title('Red Zone Opportunity Distribution', fontweight='bold')
        ax2.set_xticks(x_pos)
        ax2.set_xticklabels(player_names, rotation=45, ha='right')
        ax2.legend()
        ax2.grid(True, alpha=0.3, axis='y')
        
        plt.suptitle(f'{team} {position} Red Zone Analysis ({selected_year})', 
                    fontsize=14, fontweight='bold')
        plt.tight_layout()
        
        filename = f'{position}_red_zone_{selected_year}.png'
        filepath = plots_dir / filename
        plt.savefig(filepath, dpi=300, bbox_inches='tight', facecolor='white')
        plt.close(fig)
        
        return str(filepath.relative_to('nickknows/static/'))
        
    except Exception as e:
        logger.error(f"Error creating Red Zone plot: {str(e)}")
        return None