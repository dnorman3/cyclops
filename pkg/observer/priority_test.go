package observer

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	atlassianv1 "github.com/atlassian-labs/cyclops/pkg/apis/atlassian/v1"
)

// Helpers
func buildNodeGroup(name string, priority int32) *atlassianv1.NodeGroup {
    return &atlassianv1.NodeGroup{
        ObjectMeta: v1.ObjectMeta{
            Name: name,
        },
        Spec: atlassianv1.NodeGroupSpec{
            NodeGroupName: name,
            NodeSelector:  v1.LabelSelector{},
            Priority:      priority,
        },
    }
}

func buildListedNodeGroup(name string, priority int32, nodeNames []string) *ListedNodeGroups {
    ng := buildNodeGroup(name, priority)
    var kubeNodes []*corev1.Node
    for _, n := range nodeNames {
        kubeNodes = append(kubeNodes, &corev1.Node{ObjectMeta: v1.ObjectMeta{Name: n}})
    }
    return &ListedNodeGroups{NodeGroup: ng, List: kubeNodes, Reason: "test"}
}

// --- Tests ---

func Test_getPriority_NormalisesNegativeToZero(t *testing.T) {
    c := controller{metrics: newMetrics()}
    ng := buildNodeGroup("ng-neg", -5)

    got := c.getPriority(ng)
    assert.Equal(t, int32(0), got)
}

func Test_getPriority_ReturnsPositiveAsIs(t *testing.T) {
    c := controller{metrics: newMetrics()}
    for _, p := range []int32{0, 1, 10, 1000} {
        ng := buildNodeGroup(fmt.Sprintf("ng-%d", p), p)
        assert.Equal(t, p, c.getPriority(ng))
    }
}

func Test_validatePriority_Bounds(t *testing.T) {
    c := controller{metrics: newMetrics()}
    assert.False(t, c.validatePriority(-1))
    assert.True(t, c.validatePriority(0))
	assert.True(t, c.validatePriority(500))
    assert.True(t, c.validatePriority(1000))
    assert.False(t, c.validatePriority(1001))
}

func Test_groupByPriority_GroupsNodeGroupsCorrectly(t *testing.T) {
    c := controller{metrics: newMetrics()}
    groups := []*ListedNodeGroups{
        buildListedNodeGroup("a", 0, []string{"n1"}),
        buildListedNodeGroup("b", 0, []string{"n2"}),
        buildListedNodeGroup("c", 1, []string{"n3"}),
        buildListedNodeGroup("d", 2, []string{"n4"}),
    }

    got := c.groupByPriority(groups)
    assert.Len(t, got, 3)
    assert.Len(t, got[0], 2)
    assert.Len(t, got[1], 1)
    assert.Len(t, got[2], 1)
}

func Test_groupCNRsByPriority_Simple(t *testing.T) {
    c := controller{metrics: newMetrics()}
    cnrs := []atlassianv1.CycleNodeRequest{
        {Spec: atlassianv1.CycleNodeRequestSpec{Priority: 0}},
        {Spec: atlassianv1.CycleNodeRequestSpec{Priority: 1}},
        {Spec: atlassianv1.CycleNodeRequestSpec{Priority: 1}},
        {Spec: atlassianv1.CycleNodeRequestSpec{Priority: 2}},
    }

    got := c.groupCNRsByPriority(cnrs)
    assert.Len(t, got, 3)
    assert.Len(t, got[0], 1)
    assert.Len(t, got[1], 2)
    assert.Len(t, got[2], 1)
}

func Test_isPriorityLevelHealthy_AllSuccessful_NoHealthChecks(t *testing.T) {
    c := controller{metrics: newMetrics()}
    groups := map[int32][]atlassianv1.CycleNodeRequest{
        0: {
            {Status: atlassianv1.CycleNodeRequestStatus{Phase: atlassianv1.CycleNodeRequestSuccessful}},
            {Status: atlassianv1.CycleNodeRequestStatus{Phase: atlassianv1.CycleNodeRequestSuccessful}},
        },
    }

    assert.True(t, c.isPriorityLevelHealthy(0, groups))
}

func Test_isPriorityLevelHealthy_FailsOnNonSuccessful(t *testing.T) {
    c := controller{metrics: newMetrics()}
    groups := map[int32][]atlassianv1.CycleNodeRequest{
        0: {
            {Status: atlassianv1.CycleNodeRequestStatus{Phase: atlassianv1.CycleNodeRequestPending}},
            {Status: atlassianv1.CycleNodeRequestStatus{Phase: atlassianv1.CycleNodeRequestSuccessful}},
        },
    }

    assert.False(t, c.isPriorityLevelHealthy(0, groups))
}

func Test_isPriorityLevelHealthy_FailsOnHealthChecks(t *testing.T) {
    c := controller{metrics: newMetrics()}
    groups := map[int32][]atlassianv1.CycleNodeRequest{
        1: {
            {
                Spec: atlassianv1.CycleNodeRequestSpec{Priority: 1, HealthChecks: []atlassianv1.HealthCheck{{}}},
                Status: atlassianv1.CycleNodeRequestStatus{HealthChecks: map[string]atlassianv1.HealthCheckStatus{
                    "nodehash": {Checks: []bool{true, false}},
                }},
            },
        },
    }

    assert.False(t, c.isPriorityLevelHealthy(1, groups))
}

func Test_canActivatePriorityLevel_P0AlwaysTrue(t *testing.T) {
    c := controller{metrics: newMetrics()}
    groups := map[int32][]atlassianv1.CycleNodeRequest{
        0: {{Spec: atlassianv1.CycleNodeRequestSpec{Priority: 0}}},
    }
    assert.True(t, c.canActivatePriorityLevel(0, groups))
}

func Test_canActivatePriorityLevel_BlockedByLowerPriorityUnhealthy(t *testing.T) {
    c := controller{metrics: newMetrics()}
    groups := map[int32][]atlassianv1.CycleNodeRequest{
        0: {{Status: atlassianv1.CycleNodeRequestStatus{Phase: atlassianv1.CycleNodeRequestPending}}},
        1: {{Spec: atlassianv1.CycleNodeRequestSpec{Priority: 1}}},
    }
    assert.False(t, c.canActivatePriorityLevel(1, groups))
}

func Test_canActivatePriorityLevel_TrueWhenAllLowerHealthy(t *testing.T) {
    c := controller{metrics: newMetrics()}
    groups := map[int32][]atlassianv1.CycleNodeRequest{
        0: {{Status: atlassianv1.CycleNodeRequestStatus{Phase: atlassianv1.CycleNodeRequestSuccessful}}},
        1: {{Spec: atlassianv1.CycleNodeRequestSpec{Priority: 1}}},
    }
    assert.True(t, c.canActivatePriorityLevel(1, groups))
}

func Test_findNextActivatablePriority_Scenarios(t *testing.T) {
    c := controller{metrics: newMetrics()}

    groupsHasP0 := map[int32][]atlassianv1.CycleNodeRequest{
        0: {{Spec: atlassianv1.CycleNodeRequestSpec{Priority: 0}}},
        1: {{Spec: atlassianv1.CycleNodeRequestSpec{Priority: 1}}},
    }
    assert.Equal(t, int32(0), c.findNextActivatablePriority(groupsHasP0))

    groupsNoP0 := map[int32][]atlassianv1.CycleNodeRequest{
        1: {{Spec: atlassianv1.CycleNodeRequestSpec{Priority: 1}}},
        2: {{Spec: atlassianv1.CycleNodeRequestSpec{Priority: 2}}},
    }
    // With missing lower levels treated as healthy, lowest existing activatable level is returned
    assert.Equal(t, int32(1), c.findNextActivatablePriority(groupsNoP0))
}

func Test_isPriorityLevelHealthy_MissingLevelIsHealthy(t *testing.T) {
    c := controller{metrics: newMetrics()}
    groups := map[int32][]atlassianv1.CycleNodeRequest{
        // Only level 0 exists and is successful; check missing level 2
        0: {{Status: atlassianv1.CycleNodeRequestStatus{Phase: atlassianv1.CycleNodeRequestSuccessful}}},
    }
    assert.True(t, c.isPriorityLevelHealthy(2, groups))
}

func Test_nodeListsMatch_Cases(t *testing.T) {
    c := controller{metrics: newMetrics()}
    assert.True(t, c.nodeListsMatch([]string{"a", "b"}, []string{"a", "b"}))
    assert.True(t, c.nodeListsMatch([]string{"a", "b"}, []string{"b", "a"}))
    assert.False(t, c.nodeListsMatch([]string{"a"}, []string{"a", "b"}))
    assert.False(t, c.nodeListsMatch([]string{"a", "b"}, []string{"a", "c"}))
}

func Test_retryWithBackoff_Succeeds(t *testing.T) {
    c := controller{metrics: newMetrics()}
    attempts := 0
    err := c.retryWithBackoff(func() error {
        attempts++
        if attempts < 3 {
            return errors.New("fail")
        }
        return nil
    }, 5, time.Microsecond)

    assert.NoError(t, err)
    assert.Equal(t, 3, attempts)
}

func Test_retryWithBackoff_Exhausts(t *testing.T) {
    c := controller{metrics: newMetrics()}
    attempts := 0
    err := c.retryWithBackoff(func() error {
        attempts++
        return errors.New("always fail")
    }, 2, time.Microsecond)

    assert.Error(t, err)
    // attempts = maxRetries + 1
    assert.Equal(t, 3, attempts)
}

func Test_activateCNRWithRetry_Success(t *testing.T) {
    scheme, _ := atlassianv1.SchemeBuilder.Build()
    cnr := &atlassianv1.CycleNodeRequest{
        ObjectMeta: v1.ObjectMeta{Name: "cnr-1", Namespace: "kube-system"},
        Spec:       atlassianv1.CycleNodeRequestSpec{Priority: 1},
        Status:     atlassianv1.CycleNodeRequestStatus{Phase: atlassianv1.CycleNodeRequestUndefined},
    }

    cl := NewFakeClientWithScheme(scheme, cnr)
    c := controller{client: cl, metrics: newMetrics()}

    err := c.activateCNRWithRetry(cnr, 1)
    assert.NoError(t, err)
    assert.Equal(t, atlassianv1.CycleNodeRequestPending, cnr.Status.Phase)

    // Also fetch from client to ensure Update persisted
    var fromClient atlassianv1.CycleNodeRequest
    _ = cl.Get(context.TODO(), client.ObjectKey{Namespace: "kube-system", Name: "cnr-1"}, &fromClient)
    assert.Equal(t, atlassianv1.CycleNodeRequestPending, fromClient.Status.Phase)
}

func Test_createCNRs_DeduplicatesExactNodeLists(t *testing.T) {
    scheme, _ := atlassianv1.SchemeBuilder.Build()

    existing := &atlassianv1.CycleNodeRequest{
        ObjectMeta: v1.ObjectMeta{Name: "existing", Namespace: "kube-system"},
        Spec: atlassianv1.CycleNodeRequestSpec{
            NodeGroupName: "ng1",
            NodeNames:     []string{"n1", "n2"},
            Priority:      0,
        },
        Status: atlassianv1.CycleNodeRequestStatus{Phase: atlassianv1.CycleNodeRequestPending},
    }

    cl := NewFakeClientWithScheme(scheme, existing)
    c := controller{client: cl, metrics: newMetrics(), Options: Options{Namespace: "kube-system", CNRPrefix: "test"}}

    l := &ListedNodeGroups{
        NodeGroup: buildNodeGroup("ng1", 0),
        List: []*corev1.Node{
            {ObjectMeta: v1.ObjectMeta{Name: "n1"}},
            {ObjectMeta: v1.ObjectMeta{Name: "n2"}},
        },
        Reason: "test",
    }

    c.createCNRs([]*ListedNodeGroups{l})

    // Should still only have the pre-seeded CNR because of deduplication
    all, _ := generationListCNRs(cl)
    assert.Len(t, all.Items, 1)
}

func Test_createCNRs_DedupOrderAgnostic(t *testing.T) {
    scheme, _ := atlassianv1.SchemeBuilder.Build()

    existing := &atlassianv1.CycleNodeRequest{
        ObjectMeta: v1.ObjectMeta{Name: "existing", Namespace: "kube-system"},
        Spec: atlassianv1.CycleNodeRequestSpec{
            NodeGroupName: "ng1",
            NodeNames:     []string{"n2", "n1"}, // reversed
            Priority:      0,
        },
    }

    cl := NewFakeClientWithScheme(scheme, existing)
    c := controller{client: cl, metrics: newMetrics(), Options: Options{Namespace: "kube-system", CNRPrefix: "test"}}

    l := &ListedNodeGroups{
        NodeGroup: buildNodeGroup("ng1", 0),
        List: []*corev1.Node{
            {ObjectMeta: v1.ObjectMeta{Name: "n1"}},
            {ObjectMeta: v1.ObjectMeta{Name: "n2"}},
        },
    }

    c.createCNRs([]*ListedNodeGroups{l})

    all, _ := generationListCNRs(cl)
    assert.Len(t, all.Items, 1)
}

func Test_createCNRs_InvalidPriorityIsSkipped(t *testing.T) {
    scheme, _ := atlassianv1.SchemeBuilder.Build()
    cl := NewFakeClientWithScheme(scheme)
    c := controller{client: cl, metrics: newMetrics(), Options: Options{Namespace: "kube-system", CNRPrefix: "test"}}

    // Priority > 1000 should be skipped entirely
    l := &ListedNodeGroups{
        NodeGroup: buildNodeGroup("ng-bad", 1001),
        List: []*corev1.Node{{ObjectMeta: v1.ObjectMeta{Name: "n1"}}},
    }

    c.createCNRs([]*ListedNodeGroups{l})

    all, _ := generationListCNRs(cl)
    assert.Len(t, all.Items, 0)
}

// generationListCNRs wraps generation.ListCNRs with our fake client
func generationListCNRs(c client.Client) (*atlassianv1.CycleNodeRequestList, error) {
    var list atlassianv1.CycleNodeRequestList
    if err := c.List(context.TODO(), &list, &client.ListOptions{}); err != nil {
        return nil, err
    }
    return &list, nil
}


