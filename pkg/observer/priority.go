package observer

import (
	"context"

	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/atlassian-labs/cyclops/pkg/apis/atlassian/v1"
	"github.com/atlassian-labs/cyclops/pkg/generation"
)

// groupByPriority groups node groups by their priority
func (c *controller) groupByPriority(changedNodeGroups []*ListedNodeGroups) map[int32][]*ListedNodeGroups {
	priorityGroups := make(map[int32][]*ListedNodeGroups)

	for _, nodeGroup := range changedNodeGroups {
		priority := c.getPriority(nodeGroup.NodeGroup)
		priorityGroups[priority] = append(priorityGroups[priority], nodeGroup)
	}

	return priorityGroups
}

// createCNRs creates CNRs with priority-based staging
func (c *controller) createCNRs(changedNodeGroups []*ListedNodeGroups) {
	priorityGroups := c.groupByPriority(changedNodeGroups)

	// Create ALL CNRs upfront - priority 0 starts immediately, others wait
	for priority, groups := range priorityGroups {
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
	// Check if NodeGroup has priority field set
	if nodeGroup.Spec.Priority > 0 {
		return nodeGroup.Spec.Priority
	}

	// Default to priority 0 for backward compatibility
	return 0
}

// findNextActivatablePriority finds the lowest priority that can be activated
func (c *controller) findNextActivatablePriority(allCNRs []v1.CycleNodeRequest) int32 {
	// Find the highest priority level that exists
	maxPriority := int32(-1)
	for _, cnr := range allCNRs {
		if cnr.Spec.Priority > maxPriority {
			maxPriority = cnr.Spec.Priority
		}
	}
	
	// Check priorities in order from 0 to maxPriority
	for priority := int32(0); priority <= maxPriority; priority++ {
		if c.canActivatePriorityLevel(priority, allCNRs) {
			return priority
		}
	}
	return -1 // No priority can be activated
}

// canActivatePriorityLevel checks if all CNRs at a priority level can be activated
func (c *controller) canActivatePriorityLevel(priority int32, allCNRs []v1.CycleNodeRequest) bool {
	// Get all CNRs at this priority level
	priorityCNRs := c.getCNRsAtPriority(priority, allCNRs)
	if len(priorityCNRs) == 0 {
		return false // No CNRs at this priority
	}

	// Priority 0: Always activate immediately
	if priority == 0 {
		return true
	}

	// Higher priorities: Wait for all lower priorities to complete and be healthy
	for checkPriority := int32(0); checkPriority < priority; checkPriority++ {
		if !c.isPriorityLevelHealthy(checkPriority) {
			return false
		}
	}

	return true
}

// activatePriorityLevel activates all CNRs at a specific priority level
func (c *controller) activatePriorityLevel(priority int32, allCNRs []v1.CycleNodeRequest) {
	priorityCNRs := c.getCNRsAtPriority(priority, allCNRs)

	klog.V(1).Infof("Activating priority level %d with %d CNRs",
		priority, len(priorityCNRs))

	for _, cnr := range priorityCNRs {
		// Activate the CNR
		generation.ActivateCNR(&cnr)

		// Update the CNR in the cluster
		if err := c.client.Update(context.Background(), &cnr); err != nil {
			klog.Errorf("Failed to activate CNR %s: %v", cnr.Name, err)
		} else {
			klog.V(2).Infof("Activated CNR %s (priority %d)", cnr.Name, priority)
		}
	}
}

// checkAndActivateNextPriority finds and activates the next available priority level
func (c *controller) checkAndActivateNextPriority() {
	cnrs, err := generation.ListCNRs(c.client, &client.ListOptions{})
	if err != nil {
		klog.Errorf("failed to list CNRs: %v", err)
		return
	}

	// Find the lowest priority CNR that can be activated
	nextPriority := c.findNextActivatablePriority(cnrs.Items)
	if nextPriority == -1 {
		return // No CNRs can be activated
	}

	// Activate all CNRs at this priority level
	c.activatePriorityLevel(nextPriority, cnrs.Items)
}

// getCNRsAtPriority gets all CNRs at a specific priority level
func (c *controller) getCNRsAtPriority(priority int32, allCNRs []v1.CycleNodeRequest) []v1.CycleNodeRequest {
	var priorityCNRs []v1.CycleNodeRequest

	for _, cnr := range allCNRs {
		if cnr.Spec.Priority == priority {
			priorityCNRs = append(priorityCNRs, cnr)
		}
	}

	return priorityCNRs
}

// isPriorityLevelHealthy checks if nodes at a priority level are healthy
func (c *controller) isPriorityLevelHealthy(priority int32) bool {
	// Get CNRs at this priority level
	cnrs, err := generation.ListCNRs(c.client, &client.ListOptions{})
	if err != nil {
		klog.Errorf("failed to list CNRs for health check: %v", err)
		return false
	}

	priorityCNRs := c.getCNRsAtPriority(priority, cnrs.Items)

	for _, cnr := range priorityCNRs {
		// Check if CNR is complete
        if cnr.Status.Phase != v1.CycleNodeRequestSuccessful {
            klog.V(2).Infof("CNR %s not yet complete (phase: %s)", cnr.Name, cnr.Status.Phase)
            return false
        }

		// Check if health checks are configured and passed
		if len(cnr.Spec.HealthChecks) > 0 {
			if !c.checkCyclopsHealthChecks(&cnr) {
				klog.V(2).Infof("Health checks not passed for CNR %s", cnr.Name)
				return false
			}
		}
	}

	klog.V(2).Infof("All CNRs at priority %d are complete and healthy", priority)
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