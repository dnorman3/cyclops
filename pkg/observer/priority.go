package observer

import (
	"context"
	"fmt"
	"time"

	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/atlassian-labs/cyclops/pkg/apis/atlassian/v1"
	"github.com/atlassian-labs/cyclops/pkg/generation"
)

// recordPriorityMetrics records priority-related metrics
func (c *controller) recordPriorityMetrics(priority int32, action string) {
	c.PriorityActions.WithLabelValues(fmt.Sprintf("%d", priority), action).Inc()
}

// recordPriorityHealth records the health status of a priority level
func (c *controller) recordPriorityHealth(priority int32, healthy bool) {
	value := 0.0
	if healthy {
		value = 1.0
	}
	c.PriorityLevelHealth.WithLabelValues(fmt.Sprintf("%d", priority)).Set(value)
}

// recordPriorityActivationDuration records the time taken to activate a priority level
func (c *controller) recordPriorityActivationDuration(priority int32, duration time.Duration) {
	c.PriorityActivationDuration.WithLabelValues(fmt.Sprintf("%d", priority)).Observe(duration.Seconds())
}

// groupByPriority groups node groups by their priority
func (c *controller) groupByPriority(changedNodeGroups []*ListedNodeGroups) map[int32][]*ListedNodeGroups {
	priorityGroups := make(map[int32][]*ListedNodeGroups)

	for _, nodeGroup := range changedNodeGroups {
		priority := c.getPriority(nodeGroup.NodeGroup)
		priorityGroups[priority] = append(priorityGroups[priority], nodeGroup)
	}

	return priorityGroups
}

// validatePriority validates that priority values are reasonable
func (c *controller) validatePriority(priority int32) bool {
    // Priority should be non-negative and reasonable
    return priority >= 0 && priority <= 1000 // Reasonable upper limit
}

// createCNRs creates CNRs with priority-based staging
func (c *controller) createCNRs(changedNodeGroups []*ListedNodeGroups) {
	priorityGroups := c.groupByPriority(changedNodeGroups)

	// Create ALL CNRs upfront - priority 0 starts immediately, others wait
	for priority, groups := range priorityGroups {
		// Validate priority
		if !c.validatePriority(priority) {
			klog.Errorf("Invalid priority %d for node groups, skipping", priority)
			continue
		}
		
		c.recordPriorityMetrics(priority, "cnr_created")
		
		for _, group := range groups {
			nodeNames := make([]string, 0, len(group.List))
			for _, node := range group.List {
				nodeNames = append(nodeNames, node.Name)
			}

			// Generate CNR
			cnr := generation.GenerateCNR(*group.NodeGroup, nodeNames, c.CNRPrefix, c.Namespace)
			generation.UseGenerateNameCNR(&cnr)
			generation.GiveReason(&cnr, group.Reason)
			generation.SetAPIVersion(&cnr, apiVersion)

			// Priority 0 starts immediately, others wait for activation
			cnr.Spec.Priority = priority

			name := generation.GetName(cnr.ObjectMeta)

			if err := generation.ApplyCNR(c.client, c.DryMode, cnr); err != nil {
				klog.Errorf("failed to apply cnr %q for nodegroup %q: %s", name, group.NodeGroup.Name, err)
			} else {
				var drymodeStr string
				if c.DryMode {
					drymodeStr = "[drymode] "
				}
				klog.V(2).Infof("%screated cnr %q for nodegroup %q (priority %d)",
					drymodeStr, name, group.NodeGroup.Name, priority)
				c.CNRsCreated.WithLabelValues(group.NodeGroup.Name).Inc()
			}
		}
	}
}

// getPriority returns the priority of a node group
func (c *controller) getPriority(nodeGroup *v1.NodeGroup) int32 {
    // Return the actual priority value
    priority := nodeGroup.Spec.Priority
    
    // For backward compatibility, ensure priority is non-negative
    if priority < 0 {
        return 0
    }
    
    return priority
}

// findNextActivatablePriority finds the lowest priority that can be activated
func (c *controller) findNextActivatablePriority(priorityGroups map[int32][]v1.CycleNodeRequest) int32 {
	// Find the highest priority level that exists
	maxPriority := int32(-1)
	for priority := range priorityGroups {
		if priority > maxPriority {
			maxPriority = priority
		}
	}

	// Check priorities in order from 0 to maxPriority
	for priority := int32(0); priority <= maxPriority; priority++ {
		if c.canActivatePriorityLevel(priority, priorityGroups) {
			return priority
		}
	}
	return -1 // No priority can be activated
}

// activatePriorityLevel activates all CNRs at a specific priority level
func (c *controller) activatePriorityLevel(priority int32, priorityCNRs []v1.CycleNodeRequest) {
	klog.V(1).Infof("Activating priority level %d with %d CNRs", priority, len(priorityCNRs))

	c.recordPriorityMetrics(priority, "activation_started")

	for _, cnr := range priorityCNRs {
		// Activate the CNR
		generation.ActivateCNR(&cnr)

		// Update the CNR in the cluster
		if err := c.client.Update(context.Background(), &cnr); err != nil {
			klog.Errorf("Failed to activate CNR %s: %v", cnr.Name, err)
			c.recordPriorityMetrics(priority, "activation_failed")
		} else {
			klog.V(2).Infof("Activated CNR %s (priority %d)", cnr.Name, priority)
			c.recordPriorityMetrics(priority, "cnr_activated")
		}
	}
	
	c.recordPriorityMetrics(priority, "activation_completed")
}



// checkAndActivateNextPriority finds and activates the next available priority level
func (c *controller) checkAndActivateNextPriority() {
	start := time.Now()
	
	// Fetch CNRs once and cache them
	cnrs, err := generation.ListCNRs(c.client, &client.ListOptions{})
	if err != nil {
		klog.Errorf("failed to list CNRs: %v", err)
		return
	}

	// Pre-group CNRs by priority for efficient lookups
	priorityGroups := c.groupCNRsByPriority(cnrs.Items)

	// Find the lowest priority CNR that can be activated
	nextPriority := c.findNextActivatablePriority(priorityGroups)	
	if nextPriority == -1 {
		return // No CNRs can be activated
	}

	priorityCNRs, exists := priorityGroups[nextPriority]
	if !exists {
		klog.Errorf("Priority group %d not found in priority groups", nextPriority)
		return
	}

	// Record activation attempt
	c.recordPriorityMetrics(nextPriority, "activation_attempted")
	
	// Activate all CNRs at this priority level
	c.activatePriorityLevel(nextPriority, priorityCNRs)
	
	// Record activation duration
	duration := time.Since(start)
	c.recordPriorityActivationDuration(nextPriority, duration)
	
	// Record successful activation
	c.recordPriorityMetrics(nextPriority, "activation_successful")
}

// groupCNRsByPriority pre-groups CNRs by priority for efficient lookups
func (c *controller) groupCNRsByPriority(allCNRs []v1.CycleNodeRequest) map[int32][]v1.CycleNodeRequest {
	priorityGroups := make(map[int32][]v1.CycleNodeRequest)
	for _, cnr := range allCNRs {
		priority := cnr.Spec.Priority
		priorityGroups[priority] = append(priorityGroups[priority], cnr)
	}
	return priorityGroups
}



// isPriorityLevelHealthy checks if nodes at a priority level are healthy
func (c *controller) isPriorityLevelHealthy(priority int32, priorityGroups map[int32][]v1.CycleNodeRequest) bool {
	priorityCNRs, exists := priorityGroups[priority]
	if !exists {
		c.recordPriorityHealth(priority, false)
		return false // No CNRs at this priority
	}

	for _, cnr := range priorityCNRs {
		// Check if CNR is complete using proper constant
		if cnr.Status.Phase != v1.CycleNodeRequestSuccessful {
			klog.V(2).Infof("CNR %s not yet complete (phase: %s)", cnr.Name, cnr.Status.Phase)
			c.recordPriorityHealth(priority, false)
			return false
		}

		// Check if health checks are configured and passed
		if len(cnr.Spec.HealthChecks) > 0 {
			if !c.checkCyclopsHealthChecks(&cnr) {
				klog.V(2).Infof("Health checks not passed for CNR %s", cnr.Name)
				c.recordPriorityHealth(priority, false)
				return false
			}
		}
	}

	klog.V(2).Infof("All CNRs at priority %d are complete and healthy", priority)
	c.recordPriorityHealth(priority, true)
	return true
}
// canActivatePriorityLevel checks if all CNRs at a priority level can be activated
func (c *controller) canActivatePriorityLevel(priority int32, priorityGroups map[int32][]v1.CycleNodeRequest) bool {
	// Get all CNRs at this priority level from pre-grouped data
	priorityCNRs, exists := priorityGroups[priority]
	if !exists || len(priorityCNRs) == 0 {
		return false // No CNRs at this priority
	}

	// Priority 0: Always activate immediately
	if priority == 0 {
		return true
	}

	// Higher priorities: Wait for all lower priorities to complete and be healthy
	for checkPriority := int32(0); checkPriority < priority; checkPriority++ {
		if !c.isPriorityLevelHealthy(checkPriority, priorityGroups) {
			return false
		}
	}

	return true
}

// checkCyclopsHealthChecks checks if Cyclops health checks have passed
func (c *controller) checkCyclopsHealthChecks(cnr *v1.CycleNodeRequest) bool {
	// Check if health checks are configured
	if len(cnr.Spec.HealthChecks) == 0 {
		return true // No health checks configured, consider healthy
	}

	// Check if all nodes have passed health checks
	for nodeHash, healthStatus := range cnr.Status.HealthChecks {
		// Check if all health checks for this node have passed
		for i, passed := range healthStatus.Checks {
			if !passed {
				klog.V(2).Infof("Health check %d for node %s has not passed", i, nodeHash)
				return false
			}
		}
	}

	return true
} 